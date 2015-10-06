package dropsite

import "net"

const (
	// Data packets are used to notify a proxy that data is waiting at a drop site.
	DataCoordPacket PacketType = iota

	// Ack (short for "acknowledge") packets are sent to acknowledge that data has been
	// successfully retrieved from a drop site or that an error occurred.
	AckCoordPacket
)

const CoordPacketTypeCount = 2

// NewCoordinationSocket wraps a network connection to create a JSONSocket used for coordination.
//
// You should always close the returned JSONSocket, even if Send() and Receive() have already been
// reporting errors.
func NewCoordinationSocket(c net.Conn) *JSONSocket {
	return NewJSONSocket(c, CoordPacketTypeCount, 1)
}
