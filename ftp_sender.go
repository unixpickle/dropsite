package dropsite

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var ErrFTPBadAck error = errors.New("received malformed ack packet")
var ErrFTPReceiverHangup error = errors.New("the receiver disconnected prematurely")

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
// This will return an error if the file transfer fails to complete.
//
// After this returns, the FTPSocket will be closed.
func (f1 *FTPSender) Run() error {
	f := ftpSender{
		FTPSender:  f1,
		chunks:     make(chan chunkInfo),
		chunkDone:  make(chan struct{}),
		onDone:     make(chan struct{}),
		cancelChan: make(chan struct{}),
	}

	defer f.FTPSocket.Close()

	f.makeAckChans()
	f.launchSenders()
	f.read()
	f.waitGroup.Wait()

	// NOTE: we have to lock this here because ack Goroutines could still be running.
	f.cancelLock.Lock()
	defer f.cancelLock.Unlock()
	return f.cancelErr
}

type ftpSender struct {
	chunksLeft int64

	*FTPSender

	ackChans []chan Packet

	waitGroup sync.WaitGroup

	chunks    chan chunkInfo
	chunkDone chan struct{}
	onDone    chan struct{}

	cancelLock sync.Mutex
	cancelled  bool
	cancelErr  error
	cancelChan chan struct{}
}

func (f *ftpSender) makeAckChans() {
	f.ackChans = make([]chan Packet, len(f.DropSites))
	for i := range f.DropSites {
		f.ackChans[i] = make(chan Packet, 1)
	}

	go func() {
		for {
			ack, err := f.FTPSocket.Receive(AckFTPPacket)
			if err != nil {
				break
			}
			dsIndex, ok := ack.Fields["drop_site"].(int)
			if !ok || dsIndex < 0 || dsIndex >= len(f.ackChans) {
				f.cancel(ErrFTPBadAck)
				f.FTPSocket.Close()
				break
			}
			f.ackChans[dsIndex] <- *ack
		}

		// NOTE: if the receive failed because the file transfer completed, then f.cancel() will
		// have no effect. If it failed because of an error during the transfer, then this will
		// expedite shutdown in the case that every sender is sleeping due to drop site errors.
		f.cancel(ErrFTPReceiverHangup)

		for i := range f.DropSites {
			close(f.ackChans[i])
		}
	}()
}

func (f *ftpSender) launchSenders() {
	f.waitGroup.Add(len(f.DropSites))
	for i := range f.DropSites {
		go func(dsIndex int) {
			defer f.waitGroup.Done()
			acks := f.ackChans[dsIndex]
			errorTimeout := time.Second
			for {
				select {
				case chunk, ok := <-f.chunks:
					if !ok {
						return
					}
					if ok, dieErr := f.sendChunk(chunk, dsIndex, acks); !ok {
						if dieErr != nil {
							f.cancel(dieErr)
							return
						}
						go func() {
							select {
							case f.chunks <- chunk:
							case <-f.cancelChan:
							}
						}()
						select {
						case <-time.After(errorTimeout):
						case <-f.cancelChan:
							return
						case <-f.onDone:
							return
						}
						errorTimeout *= 2
						if errorTimeout > f.ErrorTimeout {
							errorTimeout = f.ErrorTimeout
						}
					} else {
						errorTimeout = time.Second
						select {
						case f.chunkDone <- struct{}{}:
						case <-f.cancelChan:
							return
						}
					}
				case <-f.cancelChan:
					return
				}
			}
		}(i)
	}
}

func (f *ftpSender) read() {
	// NOTE: keep an "imaginary" chunk waiting until we hit EOF.
	f.chunksLeft = 1

	go func() {
		for {
			select {
			case <-f.chunkDone:
				if atomic.AddInt64(&f.chunksLeft, -1) == 0 {
					close(f.chunks)
					close(f.onDone)
					return
				}
			case <-f.cancelChan:
				return
			}
		}
	}()

	var dataOffset int64
	for {
		data, err := f.readChunk()
		if len(data) > 0 {
			chunk := chunkInfo{f.encryptChunk(data), hashChunk(data), dataOffset}
			dataOffset += int64(len(data))
			atomic.AddInt64(&f.chunksLeft, 1)
			select {
			case f.chunks <- chunk:
			case <-f.cancelChan:
				return
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// NOTE: this finishes the "imaginary" chunk, allowing the chunk stream to be closed
				// once all pending chunks have been sent.
				f.chunkDone <- struct{}{}
			} else {
				f.cancel(err)
			}
			return
		}
	}
}

func (f *ftpSender) sendChunk(c chunkInfo, dsIndex int, acks <-chan Packet) (ok bool, die error) {
	err := f.DropSites[dsIndex].Upload(c.encrypted)
	if err != nil {
		return false, nil
	}

	packet := Packet{DataFTPPacket, map[string]interface{}{"drop_site": dsIndex, "hash": c.hash,
		"offset": strconv.FormatInt(c.offset, 10)}}
	if err := f.FTPSocket.Send(packet); err != nil {
		return false, err
	}

	ack, readOk := <-acks
	if !readOk {
		return false, ErrFTPReceiverHangup
	} else if succ, ok := ack.Fields["success"].(bool); succ {
		return true, nil
	} else if !ok {
		f.FTPSocket.Close()
		return false, ErrFTPBadAck
	} else {
		return false, nil
	}
}

func (f *ftpSender) readChunk() ([]byte, error) {
	buf := make([]byte, f.BufferSize)
	count, err := io.ReadFull(f.Input, buf)
	return buf[:count], err
}

func (f *ftpSender) cancel(err error) {
	f.cancelLock.Lock()
	defer f.cancelLock.Unlock()
	if f.cancelled {
		return
	}
	f.cancelled = true
	f.cancelErr = err
	close(f.cancelChan)
}

func (f *ftpSender) encryptChunk(data []byte) []byte {
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
