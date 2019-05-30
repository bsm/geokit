package cellstore

import (
	"errors"
	"sync"

	"github.com/golang/geo/s2"
)

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

type Compression byte

func (c Compression) isValid() bool {
	return c >= NoCompression && c <= unknownCompression
}

const (
	NoCompression Compression = iota + 1
	SnappyCompression
	unknownCompression
)

type Options struct {
	// The size of a block. Must be >= 1KiB. Default: 16KiB.
	BlockSize int

	// The maximum number of entries per section. Must be > 0. Default: 16.
	SectionSize int

	// The compression algorithm to use. Default: SnappyCompression.
	Compression Compression

	// An optional temporary directory, used by SortWriter. Default: os.TempDir()
	TempDir string
}

func (o *Options) norm() *Options {
	var oo Options
	if o != nil {
		oo = *o
	}

	if oo.BlockSize < 1 {
		oo.BlockSize = 16 * KiB
	}
	if oo.SectionSize < 1 {
		oo.SectionSize = 16
	}
	if !oo.Compression.isValid() {
		oo.Compression = SnappyCompression
	}
	return &oo
}

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
