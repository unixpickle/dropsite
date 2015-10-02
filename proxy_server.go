package dropsite

import (
	"errors"
	"net"
	"sync"

	"github.com/unixpickle/latencystream"
)

// ProxyServer stores all the configuration info needed for a proxy server to run.
type ProxyServer struct {
	ServerConn         net.Conn
	ServerReader       latencystream.ChunkStream
	DropSites          []DropSite
	CoordinationSocket CoordinationSocket
}

// Run starts the proxy functionality having already established a server connection, a list
// of drop sites, and a coordination socket to the proxy client.
//
// After this returns, the coordination socket, server socket, and server reader will all be closed.
func (s ProxyServer) Run() {
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		s.serverToClientLoop()
		wg.Done()
	}()

	go func() {
		s.clientToServerLoop()
		wg.Done()
	}()

	wg.Wait()
}

// clientToServerLoop listens for data from the client proxy and forwards it to the server.
//
// Before this returns, it closes the server socket and the coordination socket.
// This will immediately return if both the coordination socket and the server socket are closed.
// If the coordination socket is closed, this will return once all incoming data has been written to
// the server.
// If the server socket is closed, this will return if the client tries to write more data to the
// server.
func (s ProxyServer) clientToServerLoop() {
	defer s.CoordinationSocket.Close()
	defer s.ServerConn.Close()

	for {
		dataPacket, err := s.CoordinationSocket.Receive(DataPacket)
		if err != nil {
			return
		}

		dsIndex, ok1 := dataPacket.Fields["drop_site"].(int)
		hash, ok2 := dataPacket.Fields["hash"].(string)
		if !ok1 || !ok2 {
			return
		}

		data, err := s.DropSites[dsIndex].Download()
		if err == nil {
			if hashChunk(data) != hash {
				err = errors.New("hashes do not match")
			}
		}
		if err != nil {
			errPacket := Packet{AckPacket, map[string]interface{}{"error": err.Error(),
				"success": false}}
			if s.CoordinationSocket.Send(errPacket) != nil {
				return
			}
		} else {
			ack := Packet{AckPacket, map[string]interface{}{"success": true}}
			if s.CoordinationSocket.Send(ack) != nil {
				return
			}
		}
	}
}

// serverToClientLoop listens for data from the server and forwards it to the client proxy.
//
// Before this returns, it closes the server socket, s.ServerReader, and the coordination socket.
// This will return immediately if both the server socket and the coordination socket are closed.
// If the coordination socket is closed, this will return if the server tries to send data to the
// client.
// If the server socket is closed, this will return once all buffered data has been read from it and
// sent to the client.
func (s ProxyServer) serverToClientLoop() {
	defer close(s.ServerReader.Close)
	defer s.CoordinationSocket.Close()
	defer s.ServerConn.Close()

	for chunk := range s.ServerReader.Chunks {
		hash := hashChunk(chunk)
		for {
			dsIndex, ok := s.allocDropSite()
			if !ok {
				return
			}
			dropSite := s.DropSites[dsIndex]

			if err := dropSite.Upload(chunk); err != nil {
				errPacket := Packet{UploadErrorPacket, map[string]interface{}{"error": err.Error()}}
				if s.CoordinationSocket.Send(errPacket) != nil {
					return
				}
				continue
			}

			dataPacket := Packet{DataPacket, map[string]interface{}{"drop_site": dsIndex,
				"hash": hash}}
			if s.CoordinationSocket.Send(dataPacket) != nil {
				return
			}

			ack, err := s.CoordinationSocket.Receive(AckPacket)
			if err != nil {
				return
			} else if succ, ok := ack.Fields["success"].(bool); succ {
				break
			} else if !ok {
				return
			}
		}
	}
}

func (s ProxyServer) allocDropSite() (int, bool) {
	if s.CoordinationSocket.Send(Packet{AllocDropSitePacket, nil}) != nil {
		return 0, false
	}
	response, err := s.CoordinationSocket.Receive(UseDropSitePacket)
	if err != nil {
		return 0, false
	}
	dsIndex, ok := response.Fields["drop_site"].(int)
	if !ok {
		return 0, false
	} else if dsIndex < 0 || dsIndex >= len(s.DropSites) {
		return 0, false
	}
	return dsIndex, true
}
