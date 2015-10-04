package dropsite

import "net"

const (
	// StartXfer packets are used to initiate a file transfer and determine who will be the sender
	// and the receiver.
	StartXferFTPPacket PacketType = iota

	// Data packets are sent by the sender to indicate that data is available at a drop site.
	DataFTPPacket

	// Ack packets are sent by the client to indicate success or failure of data retrieval.
	AckFTPPacket
)

const FTPPacketTypeCount = 3

// NewFTPSocket wraps a network connection to create a JSONSocket used for FTP coordination.
//
// You should always close the returned JSONSocket, even if Send() and Receive() have already been
// reporting errors.
func NewFTPSocket(c net.Conn) *JSONSocket {
	return NewJSONSocket(c, CoordPacketTypeCount)
}
