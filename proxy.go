package dropsite

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/unixpickle/latencystream"
)

// Proxy stores all the configuration info needed for a proxy client or proxy server to run.
type Proxy struct {
	ClientConn         net.Conn
	ClientReader       latencystream.ChunkStream
	DropSites          []DropSite
	RemoteDropSites    []DropSite
	CoordinationSocket *JSONSocket
	MaxErrorTimeout    time.Duration
}

type proxy struct {
	*Proxy

	lastChunk   []byte
	lastHash    string
	lastDsIndex int

	allocator *allocator
}

// Run starts the proxy functionality having already established a server connection, a list
// of drop sites, and a coordination socket to the proxy server.
//
// After this returns, the coordination socket, client socket, and client reader will all be closed.
func (info Proxy) Run() {
	var wg sync.WaitGroup
	wg.Add(2)

	allocator := newAllocator(info.MaxErrorTimeout, len(info.DropSites))
	p := &proxy{&info, nil, "", 0, allocator}

	go func() {
		p.serverToClientLoop()
		wg.Done()
	}()

	go func() {
		p.clientToServerLoop()
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
func (p *proxy) clientToServerLoop() {
	defer close(p.ClientReader.Close)
	defer p.CoordinationSocket.Close()
	defer p.ClientConn.Close()

	for chunk := range p.ClientReader.Chunks {
		hash := hashChunk(chunk)
		for {
			if ok, retry := p.sendNextChunk(chunk, hash); ok {
				continue
			} else if !retry {
				return
			}
		}
	}

	p.flush()
}

// serverToClientLoop listens for data from the server proxy and forwards it to the client.
//
// Before this returns, it closes the client socket and the coordination socket.
// This will return promptly if both the coordination socket and the client socket are closed.
// If the coordination socket is closed, this will return once all incoming data has been written to
// the client.
// If the client socket is closed, this will return if the server tries to write more data to the
// client.
func (p *proxy) serverToClientLoop() {
	defer p.CoordinationSocket.Close()
	defer p.ClientConn.Close()

	for {
		dataPacket, err := p.CoordinationSocket.Receive(DataCoordPacket)

		if err != nil {
			return
		}

		dsIndex, ok1 := dataPacket.Fields["drop_site"].(int)
		hash, ok2 := dataPacket.Fields["hash"]
		if !ok1 || !ok2 {
			return
		}

		data, err := p.RemoteDropSites[dsIndex].Download()
		if err == nil && hashChunk(data) != hash {
			err = errors.New("hashes do not match")
		}
		if err != nil {
			errPacket := Packet{AckCoordPacket, map[string]interface{}{"error": err.Error(),
				"success": false}}
			if p.CoordinationSocket.Send(errPacket) != nil {
				return
			}
		} else {
			ack := Packet{AckCoordPacket, map[string]interface{}{"success": true}}
			if p.CoordinationSocket.Send(ack) != nil {
				return
			}
			if _, err := p.ClientConn.Write(data); err != nil {
				return
			}
		}
	}
}

func (p *proxy) sendNextChunk(nextChunk []byte, hash string) (ok, retry bool) {
	nextDsIndex := p.allocator.Alloc()
	dropSite := p.DropSites[nextDsIndex]
	if err := dropSite.Upload(nextChunk); err != nil {
		p.allocator.Failed(nextDsIndex)
		return false, true
	}

	if p.lastChunk != nil {
		ack, err := p.CoordinationSocket.Receive(AckCoordPacket)
		if err != nil {
			p.allocator.Failed(p.lastDsIndex)
			p.allocator.Failed(nextDsIndex)
			return false, false
		} else if succ, ok := ack.Fields["success"].(bool); !ok {
			p.allocator.Failed(p.lastDsIndex)
			p.allocator.Failed(nextDsIndex)
			return false, false
		} else if !succ {
			p.allocator.Failed(p.lastDsIndex)
			p.allocator.Failed(nextDsIndex)
			buff := p.lastChunk
			h := p.lastHash
			p.lastChunk = nil
			for {
				if ok, retry := p.sendNextChunk(buff, h); ok {
					break
				} else if !retry {
					return false, false
				}
			}
			return false, true
		} else {
			p.allocator.Free(p.lastDsIndex, int64(len(p.lastChunk)))
		}
	}

	p.lastChunk = nextChunk
	p.lastHash = hash
	p.lastDsIndex = nextDsIndex
	dataPacket := Packet{DataCoordPacket, map[string]interface{}{"drop_site": nextDsIndex,
		"hash": hash}}
	if p.CoordinationSocket.Send(dataPacket) != nil {
		p.allocator.Failed(nextDsIndex)
		return false, false
	}

	return true, false
}

func (p *proxy) flush() {
	for {
		ack, err := p.CoordinationSocket.Receive(AckCoordPacket)
		if err != nil {
			p.allocator.Failed(p.lastDsIndex)
			return
		} else if succ, ok := ack.Fields["success"].(bool); !ok {
			p.allocator.Failed(p.lastDsIndex)
			return
		} else if !succ {
			p.allocator.Failed(p.lastDsIndex)
			buff := p.lastChunk
			h := p.lastHash
			p.lastChunk = nil
			for {
				if ok, retry := p.sendNextChunk(buff, h); ok {
					break
				} else if !retry {
					return
				}
			}
		} else {
			p.allocator.Free(p.lastDsIndex, int64(len(p.lastChunk)))
			return
		}
	}
}
