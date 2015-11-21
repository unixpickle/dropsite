package dropsite

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const KeepaliveInterval = time.Second * 30
const KeepaliveTimeout = KeepaliveInterval * 2

type PacketType int

type Packet struct {
	Type   PacketType             `json:"type"`
	Fields map[string]interface{} `json:"fields"`
}

// JSONSocket communicates packets between a two proxies.
//
// All methods may be used concurrently.
type JSONSocket struct {
	incoming   []chan Packet
	keepalives chan Packet
	outgoing   chan outgoingPacket
	conn       net.Conn

	closeLock sync.RWMutex
	closed    bool
}

// NewJSONSocket wraps a network connection to create a JSONSocket.
//
// You should always close the returned JSONSocket, even if Send() and Receive() have already been
// reporting errors.
func NewJSONSocket(c net.Conn, packetTypeCount int, incomingBuffer int) *JSONSocket {
	var res JSONSocket

	res.conn = c

	res.incoming = make([]chan Packet, packetTypeCount)
	for i := 0; i < packetTypeCount; i++ {
		res.incoming[i] = make(chan Packet, incomingBuffer)
	}
	res.keepalives = make(chan Packet, 1)
	go res.incomingLoop()

	res.outgoing = make(chan outgoingPacket)
	go res.outgoingLoop()

	go res.sendKeepalives()
	go res.readKeepalives()

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
func (c *JSONSocket) Close() error {
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
func (c *JSONSocket) Receive(t PacketType) (*Packet, error) {
	if t < 0 || t >= PacketType(len(c.incoming)) {
		panic("invalid packet type")
	}
	res, ok := <-c.incoming[t]
	if !ok {
		return nil, io.EOF
	} else {
		return &res, nil
	}
}

// Receive2 reads the next packet of either of two types.
// It blocks until the packet is available or returns an error if the socket is dead.
func (c *JSONSocket) Receive2(t1, t2 PacketType) (*Packet, error) {
	if t1 < 0 || t1 >= PacketType(len(c.incoming)) || t2 < 0 || t2 >= PacketType(len(c.incoming)) {
		panic("invalid packet type")
	}
	var res Packet
	var ok bool
	select {
	case res, ok = <-c.incoming[t1]:
	case res, ok = <-c.incoming[t2]:
	}
	if !ok {
		return nil, io.EOF
	} else {
		return &res, nil
	}
}

// Send sends a packet.
// You should not modify a packet while it is being sent.
// It blocks until the packet has sent or returns an error if the socket is dead.
func (c *JSONSocket) Send(p Packet) error {
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
// When it returns, it closes all input streams and the overall JSONSocket.
func (c *JSONSocket) incomingLoop() {
	defer func() {
		c.conn.Close()
		for i := 0; i < len(c.incoming); i++ {
			close(c.incoming[i])
		}
		close(c.keepalives)
	}()

	decoder := json.NewDecoder(c.conn)
	decoder.UseNumber()
	for {
		var packet Packet
		if err := decoder.Decode(&packet); err != nil {
			return
		}

		if packet.Type == -1 {
			c.keepalives <- packet
			continue
		}

		if packet.Type < 0 || packet.Type >= PacketType(len(c.incoming)) {
			return
		}

		for key, val := range packet.Fields {
			if num, ok := val.(json.Number); ok {
				intVal, err := num.Int64()
				if err != nil {
					return
				}
				packet.Fields[key] = int(intVal)
			}
		}

		select {
		case c.incoming[packet.Type] <- packet:
		default:
			// NOTE: this means that the server sent more than the allowed number of packets of the
			// same type in sequence. This is not valid under the JSON socket protocol.
			return
		}
	}
}

// outgoingLoop encodes packets and sends them to the connection.
// It will return when c.outgoing is closed.
func (c *JSONSocket) outgoingLoop() {
	encoder := json.NewEncoder(c.conn)
	for packet := range c.outgoing {
		if err := encoder.Encode(packet.p); err != nil {
			packet.c <- err
		} else {
			close(packet.c)
		}
	}
}

// sendKeepalives periodically sends keep-alive packets until the socket is closed.
func (c *JSONSocket) sendKeepalives() {
	p := Packet{-1, map[string]interface{}{}}
	for {
		time.Sleep(KeepaliveInterval)
		if c.Send(p) != nil {
			break
		}
	}
}

// readKeepalives kills the connection if no keep-alive packets come in for too long.
func (c *JSONSocket) readKeepalives() {
	for {
		select {
		case <-time.After(KeepaliveTimeout):
			c.Close()
			return
		case _, ok := <-c.keepalives:
			if !ok {
				return
			}
		}
	}
}

type outgoingPacket struct {
	p Packet
	c chan error
}
