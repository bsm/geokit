package cellstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/golang/geo/s2"
	"github.com/golang/snappy"
)

// Reader represents a cellstore reader
type Reader struct {
	r io.ReaderAt

	index       []blockInfo
	indexOffset int64
	compression Compression
	bufPool     sync.Pool
}

// NewReader opens a reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	tmp := make([]byte, 16+binary.MaxVarintLen64)

	// read footer
	footerOffset := size - 24
	if _, err := r.ReadAt(tmp[:24], footerOffset); err != nil {
		return nil, err
	}

	// parse footer
	if !bytes.Equal(tmp[16:24], magic) {
		return nil, errBadMagic
	}
	flagsOffset := int64(binary.BigEndian.Uint64(tmp[8:]))
	indexOffset := int64(binary.BigEndian.Uint64(tmp[0:]))

	// read flags
	var flags []byte
	if n := int(footerOffset - flagsOffset); n <= cap(tmp) {
		flags = tmp[:n]
	} else {
		flags = make([]byte, n)
	}
	if _, err := r.ReadAt(flags, flagsOffset); err != nil {
		return nil, err
	}

	// parse flags
	var (
		compression Compression
	)
	for n := 0; n < len(flags); {
		code := flags[n]
		switch code {
		case flagCompression:
			if n+1 >= len(flags) {
				return nil, errBadFlags
			}
			compression = Compression(flags[n+1])
			n += 2
		default:
			return nil, fmt.Errorf("cellstore: unknown flag %d", code)
		}
	}

	// validate flags
	if !compression.isValid() {
		return nil, errInvalidCompression
	}

	// read index
	var index []blockInfo
	for pos := indexOffset; pos < flagsOffset; {
		tmp = tmp[:8+binary.MaxVarintLen64]
		if x := flagsOffset - pos; x < int64(len(tmp)) {
			tmp = tmp[:int(x)]
		}

		_, err := r.ReadAt(tmp, pos)
		if err != nil {
			return nil, err
		}

		cellID := binary.BigEndian.Uint64(tmp)
		pos += 8

		offset, n := binary.Uvarint(tmp[8:])
		pos += int64(n)

		index = append(index, blockInfo{
			MaxCellID: s2.CellID(cellID),
			Offset:    int64(offset),
		})
	}

	return &Reader{
		r: r,

		index:       index,
		indexOffset: indexOffset,
		compression: compression,
	}, nil
}

// NumBlocks returns the number of stored blocks.
func (r *Reader) NumBlocks() int {
	return len(r.index)
}

func (r *Reader) blockOffset(pos int) int64 {
	if pos < len(r.index) {
		return r.index[pos].Offset
	}
	return r.indexOffset
}

// FindBlock returns the block which is closest to the given cellID.
func (r *Reader) FindBlock(cellID s2.CellID) (*Iterator, error) {
	if !cellID.IsValid() {
		return nil, errInvalidCellID
	}

	if len(r.index) == 0 {
		return &Iterator{}, nil
	}

	pos := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxCellID >= cellID
	})
	if pos >= len(r.index) {
		pos = len(r.index) - 1
	}

	return r.readBlock(pos)
}

func (r *Reader) fetchBuffer(sz int) []byte {
	if v := r.bufPool.Get(); v != nil {
		if p := v.([]byte); sz <= cap(p) {
			return p[:sz]
		}
	}
	return make([]byte, sz)
}

func (r *Reader) readBlock(pos int) (*Iterator, error) {
	if pos < 0 || pos >= len(r.index) {
		return nil, ErrBlockUnavailable
	}

	min := r.index[pos].Offset
	max := r.indexOffset
	if next := pos + 1; next < len(r.index) {
		max = r.index[next].Offset
	}

	raw := r.fetchBuffer(int(max - min))
	if _, err := r.r.ReadAt(raw, min); err != nil {
		r.bufPool.Put(raw)
		return nil, err
	}

	var buf []byte
	if r.compression == SnappyCompression {
		sz, err := snappy.DecodedLen(raw)
		if err != nil {
			r.bufPool.Put(raw)
			return nil, err
		}

		tmp := r.fetchBuffer(sz)
		if buf, err = snappy.Decode(tmp, raw); err != nil {
			r.bufPool.Put(raw)
			r.bufPool.Put(tmp)
			return nil, err
		}
		r.bufPool.Put(raw)
	} else {
		buf = raw
	}

	return &Iterator{
		parent: r,
		buf:    buf,
		pos:    pos,
	}, nil

}

// --------------------------------------------------------------------

type Iterator struct {
	parent *Reader
	pos    int // block position

	buf []byte
	cur int // cursor position

	err    error
	cellID s2.CellID
	value  []byte
}

// First positions the cursor at the first entry
func (i *Iterator) First() {
	i.cur = 0
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.cur+8 > len(i.buf) {
		return false
	}

	i.cellID = s2.CellID(binary.BigEndian.Uint64(i.buf[i.cur:]))
	i.cur += 8

	if i.cur+1 > len(i.buf) {
		return false
	}
	u, n := binary.Uvarint(i.buf[i.cur:])
	i.cur += n

	if i.cur+int(u) > len(i.buf) {
		return false
	}
	i.value = i.buf[i.cur : i.cur+int(u)]
	i.cur += int(u)

	return true
}

// NextBlock jumps to the next block, may return ErrBlockUnavailable if the
// block is unavailable.
func (i *Iterator) NextBlock() error {
	return i.replaceWith(i.pos + 1)
}

// PrevBlock jumps to the previous block, may return ErrBlockUnavailable if
// the block is unavailable.
func (i *Iterator) PrevBlock() error {
	return i.replaceWith(i.pos - 1)
}

func (i *Iterator) replaceWith(pos int) error {
	b, err := i.parent.readBlock(pos)
	if err != nil {
		return err
	}

	i.Release()
	*i = *b
	return nil
}

// CellID returns the cell ID of the current entry.
func (i *Iterator) CellID() s2.CellID {
	return i.cellID
}

// Value returns the value of the current entry. Please note that values
// are temporary buffers and must be copied if used beyond the next Next() or
// Release() function call.
func (i *Iterator) Value() []byte {
	return i.value
}

// Err returns iterator errors
func (i *Iterator) Err() error {
	return i.err
}

// Release releases the iterator. It must not be used once this method is called.
func (i *Iterator) Release() {
	if cap(i.buf) > 0 {
		i.parent.bufPool.Put(i.buf)
	}
}
