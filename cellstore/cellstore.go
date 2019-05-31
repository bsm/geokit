package cellstore

import (
	"errors"
	"sync"

	"github.com/golang/geo/s2"
)

// Bytes-size unit
const (
	KiB = 1024
	MiB = 1024 * KiB
)

var magic = []byte{160, 68, 149, 151, 154, 60, 56, 157}

var (
	errClosed             = errors.New("cellstore: is closed")
	errBadMagic           = errors.New("cellstore: bad magic byte sequence")
	errBadFlags           = errors.New("cellstore: bad flags section")
	errInvalidCompression = errors.New("cellstore: invalid compression setting")
	errInvalidCellID      = errors.New("cellstore: invalid cell ID")
)

const (
	blockNoCompression     = 0
	blockSnappyCompression = 1
)

// --------------------------------------------------------------------

// Compression is the compression codec
type Compression byte

func (c Compression) isValid() bool {
	return c >= NoCompression && c <= unknownCompression
}

// Supported compression codecs
const (
	NoCompression Compression = iota + 1
	SnappyCompression
	unknownCompression
)

// --------------------------------------------------------------------

type blockInfo struct {
	MaxCellID s2.CellID // maximum cell ID in the block
	Offset    int64     // block offset position
}

// --------------------------------------------------------------------

var bufPool sync.Pool

func fetchBuffer(sz int) []byte {
	if v := bufPool.Get(); v != nil {
		if p := v.([]byte); sz <= cap(p) {
			return p[:sz]
		}
	}
	return make([]byte, sz)
}

func releaseBuffer(p []byte) {
	if cap(p) != 0 {
		bufPool.Put(p)
	}
}

var intSlicePool sync.Pool

func fetchIntSlice(cp int) []int {
	if v := intSlicePool.Get(); v != nil {
		return v.([]int)[:0]
	}
	return make([]int, 0, cp)
}

func releaseIntSlice(p []int) {
	if cap(p) != 0 {
		intSlicePool.Put(p)
	}
}
