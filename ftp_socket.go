package dropsite

import "net"

const (
	// Data packets are sent by the sender to indicate that data is available at a drop site.
	DataFTPPacket PacketType = iota

	// Ack packets are sent by the client to indicate success or failure of data retrieval.
	AckFTPPacket
)

const FTPPacketTypeCount = 2

// NewFTPSocket wraps a network connection to create a JSONSocket used for FTP coordination.
//
// You should always close the returned JSONSocket, even if Send() and Receive() have already been
// reporting errors.
func NewFTPSocket(c net.Conn) *JSONSocket {
	return NewJSONSocket(c, CoordPacketTypeCount)
}
