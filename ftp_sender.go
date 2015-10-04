package dropsite

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"sync"
	"time"
)

type FTPSender struct {
	// Input is the source of data to be sent to the receiver.
	Input io.Reader

	// BufferSize indicates how much data should be uploaded at once to a given drop site.
	BufferSize int

	// DropSites is the list of drop sites to be used in parallel during the transfer.
	DropSites []DropSite

	// FTPSocket is the connection to the receiver.
	FTPSocket *JSONSocket

	// ErrorTimeout is the maximum amount of time a drop site should have to wait after an error
	// before another attempt is made to use it.
	ErrorTimeout time.Duration

	// AESKey is used to encrypt every outgoing piece of data which is uploaded to a drop site.
	AESKey []byte
}

// Run performs the file transfer.
//
// After this returns, the FTPSocket will be closed.
func (f *FTPSender) Run() {
	defer f.FTPSocket.Close()

	chunks := make(chan chunkInfo)
	doneChan := make(chan struct{})
	ackChans := f.makeAckChans()

	var wg sync.WaitGroup
	wg.Add(len(f.DropSites))
	for i := range f.DropSites {
		go func(dsIndex int) {
			defer wg.Done()
			acks := ackChans[dsIndex]
			for {
				select {
				case chunk := <-chunks:
					if ok, shouldDie := f.sendChunk(chunk, dsIndex, acks); !ok {
						if shouldDie {
							close(doneChan)
							return
						}
						go func() {
							select {
							case chunks <- chunk:
							case <-doneChan:
							}
						}()
						time.Sleep(f.ErrorTimeout)
					}
				case <-doneChan:
					return
				}
			}
		}(i)
	}

	var dataOffset int64
ReadLoop:
	for {
		data, err := f.readChunk()
		if len(data) > 0 {
			chunk := chunkInfo{f.encryptChunk(data), hashChunk(data), dataOffset}
			dataOffset += int64(len(data))
			select {
			case chunks <- chunk:
			case <-doneChan:
				break ReadLoop
			}
		}
		if err != nil {
			close(doneChan)
			break
		}
	}

	wg.Wait()
}

func (f *FTPSender) sendChunk(c chunkInfo, dsIndex int, acks <-chan Packet) (ok, shouldDie bool) {
	err := f.DropSites[dsIndex].Upload(c.encrypted)
	if err != nil {
		return false, false
	}

	packet := Packet{DataFTPPacket, map[string]interface{}{"drop_site": dsIndex, "hash": c.hash}}
	if f.FTPSocket.Send(packet) != nil {
		return false, true
	}

	ack, readOk := <-acks
	if !readOk {
		return false, true
	} else if succ, ok := ack.Fields["success"].(bool); succ {
		return true, false
	} else if !ok {
		f.FTPSocket.Close()
		return false, true
	} else {
		return false, false
	}
}

func (f *FTPSender) readChunk() ([]byte, error) {
	buf := make([]byte, f.BufferSize)
	count, err := io.ReadAtLeast(f.Input, buf, f.BufferSize)
	return buf[:count], err
}

func (f *FTPSender) makeAckChans() []chan Packet {
	ackChans := make([]chan Packet, len(f.DropSites))

	for i := range f.DropSites {
		ackChans[i] = make(chan Packet, 1)
	}

	go func() {
		for {
			ack, err := f.FTPSocket.Receive(AckFTPPacket)
			if err != nil {
				break
			}
			dsIndex, ok := ack.Fields["drop_site"].(int)
			if !ok || dsIndex < 0 || dsIndex >= len(ackChans) {
				f.FTPSocket.Close()
				break
			}
			ackChans[dsIndex] <- *ack
		}
		for i := range f.DropSites {
			close(ackChans[i])
		}
	}()
	return ackChans
}

func (f *FTPSender) encryptChunk(data []byte) []byte {
	// NOTE: result will be IV + encrypted data.
	result := make([]byte, aes.BlockSize+len(data))

	iv := result[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}

	block, err := aes.NewCipher(f.AESKey)
	if err != nil {
		panic(err)
	}
	cfb := cipher.NewCFBEncrypter(block, iv)
	cfb.XORKeyStream(result[aes.BlockSize:], data)

	return result
}

type chunkInfo struct {
	encrypted []byte
	hash      string
	offset    int64
}
