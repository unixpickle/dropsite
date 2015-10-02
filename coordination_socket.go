package dropsite

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
)

type PacketType int

const (
	// Data packets are used to notify a proxy that data is waiting at a drop site.
	DataPacket = iota

	// Ack (short for "acknowledge") packets are sent to acknowledge that data has been
	// successfully retrieved from a drop site or that an error occurred.
	AckPacket

	// AllocDropSite packets are sent by the proxy server to the proxy client to ask which drop
	// site it should use to send the next chunk of data.
	// The proxy client never sends these because it handles drop site allocation for both sides.
	AllocDropSitePacket

	// UseDropSite packets are sent by the proxy client to the proxy server as a response to an
	// AllocDropSite packet.
	UseDropSitePacket

	// UploadError packets are sent by the proxy server to indicate that it could not upload data
	// to a given drop site.
	UploadErrorPacket
)

const PacketTypeCount = 5

type Packet struct {
	Type   PacketType             `json:"type"`
	Fields map[string]interface{} `json:"fields"`
}

// CoordinationSocket communicates packets between a two proxies.
//
// All methods may be used concurrently.
type CoordinationSocket struct {
	incoming [PacketTypeCount]chan Packet
	outgoing chan outgoingPacket
	conn     net.Conn

	closeLock sync.RWMutex
	closed    bool
}

// NewCoordinationSocket wraps a network connection to create a CoordinationSocket.
//
// You should always close the returned CoordinationSocket, even if Send() and Receive() have
// already been reporting errors.
func NewCoordinationSocket(c net.Conn) *CoordinationSocket {
	var res CoordinationSocket

	res.conn = c

	for i := 0; i < PacketTypeCount; i++ {
		res.incoming[i] = make(chan Packet, 1)
	}
	go res.incomingLoop()

	res.outgoing = make(chan outgoingPacket)
	go res.outgoingLoop()

	return &res
}

// Close closes the socket.
// It will cancel any blocking Send() or Receive() calls asynchronously.
//
// After a socket is closed, all successive Send() calls will fail.
// However, it is not guaranteed that successive Receive() calls will fail.
// If incoming packets have been queued up, Receive() will return them before reporting EOF errors.
//
// This will return an error if the socket is already closed.
func (c *CoordinationSocket) Close() error {
	c.closeLock.Lock()
	defer c.closeLock.Unlock()
	if c.closed {
		return errors.New("already closed")
	}
	c.closed = true
	close(c.outgoing)
	c.conn.Close()
	return nil
}

// Receive reads the next packet of a given type.
// It blocks until the packet is available or returns an error if the socket is dead.
func (c *CoordinationSocket) Receive(t PacketType) (*Packet, error) {
	if t < 0 || t >= PacketTypeCount {
		panic("invalid packet type")
	}
	res, ok := <-c.incoming[t]
	if !ok {
		return nil, io.EOF
	} else {
		return &res, nil
	}
}

// Send sends a packet.
// You should not modify a packet while it is being sent.
// It blocks until the packet has sent or returns an error if the socket is dead.
func (c *CoordinationSocket) Send(p Packet) error {
	c.closeLock.RLock()
	defer c.closeLock.RUnlock()
	if c.closed {
		return errors.New("socket is closed")
	}
	errChan := make(chan error, 1)
	c.outgoing <- outgoingPacket{p, errChan}
	return <-errChan
}

// incomingLoop reads packets from the socket and writes them to various incoming channels.
// It will return when the socket is closed.
// It will close the socket if an invalid packet is received.
// It will close the socket if it blocks while writing packets to incoming channels, since this
// indicates an invalid packet from the remote end.
// When it returns, it closes all input streams and the overall CoordinationSocket.
func (c *CoordinationSocket) incomingLoop() {
	defer func() {
		c.conn.Close()
		for i := 0; i < PacketTypeCount; i++ {
			close(c.incoming[i])
		}
	}()

	decoder := json.NewDecoder(c.conn)
	for {
		var packet Packet
		if err := decoder.Decode(&packet); err != nil {
			return
		}

		if packet.Type < 0 || packet.Type >= PacketTypeCount {
			return
		}

		select {
		case c.incoming[packet.Type] <- packet:
		default:
			// NOTE: this means that the server sent more than one packet of the same type in
			// sequence. This is not valid under the coordination socket protocol.
			return
		}
	}
}

// outgoingLoop encodes packets and sends them to the connection.
// It will return when c.outgoing is closed.
func (c *CoordinationSocket) outgoingLoop() {
	encoder := json.NewEncoder(c.conn)
	for packet := range c.outgoing {
		if err := encoder.Encode(packet.p); err != nil {
			packet.c <- err
		} else {
			close(packet.c)
		}
	}
}

type outgoingPacket struct {
	p Packet
	c chan error
}
