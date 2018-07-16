package cellstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/golang/geo/s2"
	"github.com/golang/snappy"
)

// Reader represents a cellstore reader
type Reader struct {
	r io.ReaderAt

	index       []blockInfo
	indexOffset int64
	compression Compression
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
	flagsOffset := int64(binary.LittleEndian.Uint64(tmp[8:]))
	indexOffset := int64(binary.LittleEndian.Uint64(tmp[0:]))

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
	var info blockInfo

	for pos := indexOffset; pos < flagsOffset; {
		tmp = tmp[:2*binary.MaxVarintLen64]
		if x := flagsOffset - pos; x < int64(len(tmp)) {
			tmp = tmp[:int(x)]
		}

		_, err := r.ReadAt(tmp, pos)
		if err != nil {
			return nil, err
		}

		u1, n := binary.Uvarint(tmp[0:])
		pos += int64(n)

		u2, n := binary.Uvarint(tmp[n:])
		pos += int64(n)

		info.MaxCellID += s2.CellID(u1)
		info.Offset += int64(u2)
		index = append(index, info)
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

	iter := &Iterator{parent: r}
	if len(r.index) == 0 {
		return iter, nil
	}

	iter.pos = sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxCellID >= cellID
	})
	if iter.pos >= len(r.index) {
		return iter, nil
	}

	var err error
	if iter.buf, err = r.readBlock(iter.pos); err != nil {
		return nil, err
	}
	return iter, nil
}

func (r *Reader) readBlock(pos int) ([]byte, error) {
	min := r.index[pos].Offset
	max := r.indexOffset
	if next := pos + 1; next < len(r.index) {
		max = r.index[next].Offset
	}

	raw := fetchBuffer(int(max - min))
	if _, err := r.r.ReadAt(raw, min); err != nil {
		releaseBuffer(raw)
		return nil, err
	}

	switch r.compression {
	case SnappyCompression:
		defer releaseBuffer(raw)

		sz, err := snappy.DecodedLen(raw)
		if err != nil {
			return nil, err
		}

		buf, err := snappy.Decode(fetchBuffer(sz), raw)
		if err != nil {
			releaseBuffer(buf)
			return nil, err
		}
		return buf, nil
	default:
		return raw, nil
	}
}

// --------------------------------------------------------------------

type Iterator struct {
	parent *Reader
	pos    int // block position

	buf []byte
	cur int // cursor position

	err error
	cid s2.CellID
	val []byte
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	// read CellID
	if i.cur+1 > len(i.buf) {
		return false
	}
	key, n := binary.Uvarint(i.buf[i.cur:])
	i.cid += s2.CellID(key)
	i.cur += n

	// read value length
	if i.cur+1 > len(i.buf) {
		return false
	}
	vln, n := binary.Uvarint(i.buf[i.cur:])
	i.cur += n

	// read value
	if i.cur+int(vln) > len(i.buf) {
		return false
	}
	i.val = i.buf[i.cur : i.cur+int(vln)]
	i.cur += int(vln)

	return true
}

// NextBlock jumps to the next block, returns true if successful.
func (i *Iterator) NextBlock() bool {
	return i.advance(i.pos + 1)
}

// PrevBlock jumps to the previous block, returns true if successful.
func (i *Iterator) PrevBlock() bool {
	return i.advance(i.pos - 1)
}

func (i *Iterator) advance(pos int) bool {
	if i.err != nil {
		return false
	}

	if pos < 0 || pos >= len(i.parent.index) {
		return false
	}

	buf, err := i.parent.readBlock(pos)
	if err != nil {
		i.err = err
		return false
	}

	releaseBuffer(i.buf)
	*i = Iterator{
		parent: i.parent,
		pos:    pos,
		buf:    buf,
	}
	return true
}

// CellID returns the cell ID of the current entry.
func (i *Iterator) CellID() s2.CellID {
	return i.cid
}

// Value returns the value of the current entry. Please note that values
// are temporary buffers and must be copied if used beyond the next Next() or
// Release() function call.
func (i *Iterator) Value() []byte {
	return i.val
}

// Err returns iterator errors
func (i *Iterator) Err() error {
	return i.err
}

// Release releases the iterator. It must not be used once this method is called.
func (i *Iterator) Release() {
	releaseBuffer(i.buf)
}
