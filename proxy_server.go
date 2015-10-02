package dropsite

import (
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
// This will surely return if both the coordination socket and the server socket are closed.
func (s ProxyServer) clientToServerLoop() {
	defer s.CoordinationSocket.Close()
	defer s.ServerConn.Close()

	for {
		// TODO: read a data packet here, etc.
	}
}

// serverToClientLoop listens for data from the server and forwards it to the client proxy.
//
// Before this returns, it closes the server socket, s.ServerReader, and the coordination socket.
// This will surely return if both the coordination socket and the server socket are closed.
func (s ProxyServer) serverToClientLoop() {
	defer close(s.ServerReader.Close)
	defer s.CoordinationSocket.Close()
	defer s.ServerConn.Close()

	for _ = range s.ServerReader.Chunks {
		if err := s.CoordinationSocket.Send(Packet{AllocDropSitePacket, nil}); err != nil {
			return
		}
		// TODO: use the drop site we allocated, upload data, etc.
	}
}
