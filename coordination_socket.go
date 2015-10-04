package dropsite

import "net"

const (
	// Data packets are used to notify a proxy that data is waiting at a drop site.
	DataCoordPacket PacketType = iota

	// Ack (short for "acknowledge") packets are sent to acknowledge that data has been
	// successfully retrieved from a drop site or that an error occurred.
	AckCoordPacket

	// AllocDropSite packets are sent by the proxy server to the proxy client to ask which drop
	// site it should use to send the next chunk of data.
	// The proxy client never sends these because it handles drop site allocation for both sides.
	AllocDropSiteCoordPacket

	// UseDropSite packets are sent by the proxy client to the proxy server as a response to an
	// AllocDropSite packet.
	UseDropSiteCoordPacket

	// UploadError packets are sent by the proxy server to indicate that it could not upload data
	// to a given drop site.
	UploadErrorCoordPacket
)

const CoordPacketTypeCount = 5

// NewCoordinationSocket wraps a network connection to create a JSONSocket used for coordination.
//
// You should always close the returned JSONSocket, even if Send() and Receive() have already been
// reporting errors.
func NewCoordinationSocket(c net.Conn) *JSONSocket {
	return NewJSONSocket(c, CoordPacketTypeCount, 1)
}
