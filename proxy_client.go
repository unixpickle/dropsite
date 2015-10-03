package dropsite

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/unixpickle/latencystream"
)

// ProxyClient stores all the configuration info needed for a proxy client to run.
type ProxyClient struct {
	ClientConn         net.Conn
	ClientReader       latencystream.ChunkStream
	DropSites          []DropSite
	CoordinationSocket CoordinationSocket
	MaxErrorTimeout    time.Duration
}

// Run starts the proxy functionality having already established a server connection, a list
// of drop sites, and a coordination socket to the proxy server.
//
// After this returns, the coordination socket, client socket, and client reader will all be closed.
func (p ProxyClient) Run() {
	var wg sync.WaitGroup
	wg.Add(2)

	allocator := newAllocator(p.MaxErrorTimeout, len(p.DropSites))

	go func() {
		p.serverToClientLoop(allocator)
		wg.Done()
	}()

	go func() {
		p.clientToServerLoop(allocator)
		wg.Done()
	}()

	wg.Wait()
}

// clientToServerLoop listens for data from the client and forwards it to the server proxy.
//
// Before this returns, it closes the client socket, s.ClientReader, and the coordination socket.
// This will return promptly if both the client socket and the coordination socket are closed.
// If the coordination socket is closed, this will return if the client tries to send data to the
// server.
// If the client socket is closed, this will return once all buffered data has been read from it and
// sent to the server proxy.
func (p ProxyClient) clientToServerLoop(a *allocator) {
	defer close(p.ClientReader.Close)
	defer p.CoordinationSocket.Close()
	defer p.ClientConn.Close()

	for chunk := range p.ClientReader.Chunks {
		hash := hashChunk(chunk)
		for {
			dsIndex := a.Alloc()
			dropSite := p.DropSites[dsIndex]

			if err := dropSite.Upload(chunk); err != nil {
				a.Failed(dsIndex)
				continue
			}

			dataPacket := Packet{DataPacket, map[string]interface{}{"drop_site": dsIndex,
				"hash": hash}}
			if p.CoordinationSocket.Send(dataPacket) != nil {
				a.Failed(dsIndex)
				return
			}

			ack, err := p.CoordinationSocket.Receive(AckPacket)
			if err != nil {
				a.Failed(dsIndex)
				return
			} else if succ, ok := ack.Fields["success"].(bool); !ok {
				a.Failed(dsIndex)
				return
			} else if succ {
				a.Free(dsIndex, int64(len(chunk)))
				break
			} else {
				a.Failed(dsIndex)
			}
		}
	}
}

// serverToClientLoop listens for data from the server proxy and forwards it to the client.
//
// Before this returns, it closes the client socket and the coordination socket.
// This will return promptly if both the coordination socket and the client socket are closed.
// If the coordination socket is closed, this will return once all incoming data has been written to
// the client.
// If the client socket is closed, this will return if the server tries to write more data to the
// client.
func (p ProxyClient) serverToClientLoop(a *allocator) {
	defer p.CoordinationSocket.Close()
	defer p.ClientConn.Close()

	for {
		_, err := p.CoordinationSocket.Receive(AllocDropSitePacket)
		if err != nil {
			return
		}

		dsIndex := a.Alloc()
		usePacket := Packet{UseDropSitePacket, map[string]interface{}{"drop_site": dsIndex}}
		if p.CoordinationSocket.Send(usePacket) != nil {
			a.Failed(dsIndex)
			return
		}

		response, err := p.CoordinationSocket.Receive2(DataPacket, UploadErrorPacket)

		if err != nil {
			a.Failed(dsIndex)
			return
		}

		if response.Type == UploadErrorPacket {
			a.Failed(dsIndex)
			continue
		}

		idx, ok1 := response.Fields["drop_site"].(int)
		hash, ok2 := response.Fields["hash"]
		if !ok1 || !ok2 || idx != dsIndex {
			a.Failed(dsIndex)
			return
		}

		data, err := p.DropSites[dsIndex].Download()
		if err == nil && hashChunk(data) != hash {
			err = errors.New("hashes do not match")
		}
		if err != nil {
			a.Failed(dsIndex)
			errPacket := Packet{AckPacket, map[string]interface{}{"error": err.Error(),
				"success": false}}
			if p.CoordinationSocket.Send(errPacket) != nil {
				return
			}
		} else {
			a.Free(dsIndex, int64(len(data)))
			ack := Packet{AckPacket, map[string]interface{}{"success": true}}
			if p.CoordinationSocket.Send(ack) != nil {
				return
			}
			if _, err := p.ClientConn.Write(data); err != nil {
				return
			}
		}
	}
}
