package dropsite

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"io"
	"strconv"
	"sync"
)

var ErrFTPBadData error = errors.New("received malformed data packet")
var ErrFTPCouldNotDecrypt error = errors.New("drop site data could not be decrypted")

type FTPReceiver struct {
	// Output is the place to which data will be written, not necessarily sequentially.
	Output io.WriteSeeker

	// DropSites is the list of drop sites from which data should be downloaded.
	// This list must match exactly with the FTPSender's list.
	DropSites []DropSite

	// FTPSocket is the connection to the sender.
	FTPSocket *JSONSocket

	// AESKey is used to decrypt every incoming piece of data which is uploaded to a drop site.
	AESKey []byte
}

// Run performs the file transfer.
// This will return an error if the file transfer fails to complete.
//
// After this returns, the FTPSocket will be closed.
func (f *FTPReceiver) Run() error {
	defer f.FTPSocket.Close()

	var wg sync.WaitGroup
	var firstErr error
	var firstErrLock sync.Mutex

	var fileAccessLock sync.Mutex

	for {
		packet, err := f.FTPSocket.Receive(DataFTPPacket)
		if err != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := f.handleDataPacket(*packet, &fileAccessLock); err != nil {
				firstErrLock.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrLock.Unlock()
				f.FTPSocket.Close()
				return
			}
		}()
	}

	wg.Wait()
	return firstErr
}

func (f *FTPReceiver) handleDataPacket(packet Packet, fileLock *sync.Mutex) error {
	chunk, err := f.loadDataFromPacket(packet)

	if err == ErrFTPBadData {
		return err
	} else if err != nil {
		errPacket := Packet{AckFTPPacket, map[string]interface{}{"drop_site": chunk.dsIndex,
			"success": false}}
		return f.FTPSocket.Send(errPacket)
	}

	return f.saveChunkAndAck(chunk, fileLock)
}

func (f *FTPReceiver) loadDataFromPacket(packet Packet) (chunk loadedChunk, err error) {
	var ok1 bool

	chunk.dsIndex, ok1 = packet.Fields["drop_site"].(int)
	hash, ok2 := packet.Fields["hash"].(string)
	offset, ok3 := packet.Fields["offset"].(string)
	chunk.offset, err = strconv.ParseInt(offset, 10, 64)
	if !ok1 || !ok2 || !ok3 || err != nil {
		return chunk, ErrFTPBadData
	}

	chunk.data, err = f.DropSites[chunk.dsIndex].Download()
	if err == nil {
		chunk.data, err = f.decryptData(chunk.data)
		if err == nil && hashChunk(chunk.data) != hash {
			err = errors.New("bad hash")
		}
	}
	return
}

func (f *FTPReceiver) saveChunkAndAck(chunk loadedChunk, fileLock *sync.Mutex) error {
	fileLock.Lock()
	if _, err := f.Output.Seek(chunk.offset, 0); err != nil {
		fileLock.Unlock()
		return err
	}
	if _, err := f.Output.Write(chunk.data); err != nil {
		fileLock.Unlock()
		return err
	}
	fileLock.Unlock()
	ack := Packet{AckFTPPacket, map[string]interface{}{"drop_site": chunk.dsIndex,
		"success": true}}
	return f.FTPSocket.Send(ack)
}

func (f *FTPReceiver) decryptData(data []byte) ([]byte, error) {
	if len(data) < aes.BlockSize {
		return nil, ErrFTPCouldNotDecrypt
	}
	block, err := aes.NewCipher(f.AESKey)
	if err != nil {
		panic(err)
	}
	iv := data[:aes.BlockSize]
	cryptData := data[aes.BlockSize:]
	cfb := cipher.NewCFBDecrypter(block, iv)
	cfb.XORKeyStream(cryptData, cryptData)
	return cryptData, nil
}

type loadedChunk struct {
	data    []byte
	offset  int64
	dsIndex int
}
