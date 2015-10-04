package dropsite

import (
	"io"
	"sync"
	"time"
)

type FTPSender struct {
	Input        io.Reader
	BufferSize   int
	DropSites    []DropSite
	FTPSocket    *JSONSocket
	ErrorTimeout time.Duration
}

// Run performs the file transfer.
//
// After this returns, the FTPSocket will be closed.
func (f *FTPSender) Run() {
	defer f.FTPSocket.Close()

	chunks := make(chan chunkInfo)
	doneChan := make(chan struct{})

	var wg sync.WaitGroup
	for i := range f.DropSites {
		go func(dsIndex int) {
			defer wg.Done()
			for {
				select {
				case chunk := <-chunks:
					if ok, shouldDie := f.sendChunk(chunk, dsIndex); !ok {
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
		if data != nil {
			chunk := chunkInfo{data, dataOffset}
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

func (f *FTPSender) readChunk() ([]byte, error) {
	buf := make([]byte, f.BufferSize)
	gotten := 0
	for gotten < f.BufferSize {
		c, err := f.Input.Read(buf[gotten:])
		gotten += c
		if err != nil {
			if gotten > 0 {
				return buf[:gotten], err
			} else {
				return nil, err
			}
		}
	}
	return buf, nil
}

func (f *FTPSender) sendChunk(c chunkInfo, dsIndex int) (ok, shouldDie bool) {
	// TODO: send the chunk to the receiver and verify it got through.
	return
}

type chunkInfo struct {
	data   []byte
	offset int64
}
