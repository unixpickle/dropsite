package dropsite

import (
	"crypto/md5"
	"encoding/hex"
)

func hashChunk(data []byte) string {
	d := md5.Sum(data)
	return hex.EncodeToString(d[:])
}
