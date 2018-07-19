package cellstore

import (
	"bytes"
	"encoding/binary"
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
}

// NewReader opens a reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	tmp := make([]byte, 16+binary.MaxVarintLen64)

	// read footer
	footerOffset := size - 16
	if _, err := r.ReadAt(tmp[:16], footerOffset); err != nil {
		return nil, err
	}

	// parse footer
	if !bytes.Equal(tmp[8:16], magic) {
		return nil, errBadMagic
	}
	indexOffset := int64(binary.LittleEndian.Uint64(tmp[:8]))

	// read index
	var index []blockInfo
	var info blockInfo

	for pos := indexOffset; pos < footerOffset; {
		tmp = tmp[:2*binary.MaxVarintLen64]
		if x := footerOffset - pos; x < int64(len(tmp)) {
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
	}, nil
}

// NumBlocks returns the number of stored blocks.
func (r *Reader) NumBlocks() int {
	return len(r.index)
}

func (r *Reader) blockOffset(blockNo int) int64 {
	if blockNo < len(r.index) {
		return r.index[blockNo].Offset
	}
	return r.indexOffset
}

// FindBlock returns the block which is closest to the given cellID.
func (r *Reader) FindBlock(cellID s2.CellID) (*Iterator, error) {
	if !cellID.IsValid() {
		return nil, errInvalidCellID
	}

	if len(r.index) == 0 {
		return &Iterator{parent: r}, nil
	}

	blockPos := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxCellID >= cellID
	})
	if blockPos >= len(r.index) {
		return &Iterator{parent: r}, nil
	}
	return r.readBlock(blockPos)
}

func (r *Reader) readBlock(blockNo int) (*Iterator, error) {
	min := r.index[blockNo].Offset
	max := r.indexOffset
	if next := blockNo + 1; next < len(r.index) {
		max = r.index[next].Offset
	}

	raw := fetchBuffer(int(max - min))
	if _, err := r.r.ReadAt(raw, min); err != nil {
		releaseBuffer(raw)
		return nil, err
	}

	var buf []byte
	switch maxPos := len(raw) - 1; raw[maxPos] {
	case blockNoCompression:
		buf = raw[:maxPos]
	case blockSnappyCompression:
		defer releaseBuffer(raw)

		sz, err := snappy.DecodedLen(raw[:maxPos])
		if err != nil {
			return nil, err
		}

		pln := fetchBuffer(sz)
		res, err := snappy.Decode(pln, raw[:maxPos])
		if err != nil {
			releaseBuffer(pln)
			return nil, err
		}
		buf = res
	default:
		releaseBuffer(raw)
		return nil, errInvalidCompression
	}

	numSectionsOffset := len(buf) - 4
	numSections := int(binary.LittleEndian.Uint32(buf[numSectionsOffset:]))

	sectionIndexOffset := len(buf) - numSections*4
	sectionIndex := append(make([]int, 0, numSections), 0)
	for n := sectionIndexOffset; n < numSectionsOffset; n += 4 {
		sectionIndex = append(sectionIndex, int(binary.LittleEndian.Uint32(buf[n:])))
	}

	return &Iterator{
		parent:      r,
		blockNo:     blockNo,
		index:       sectionIndex,
		indexOffset: sectionIndexOffset,
		buf:         buf,
	}, nil
}

// --------------------------------------------------------------------

type Iterator struct {
	parent      *Reader
	blockNo     int   // block number
	sectionNo   int   // section number
	index       []int // section index
	indexOffset int   // section index offset

	buf []byte
	nr  int // number of buffer bytes read

	cellID s2.CellID
	value  []byte
	err    error
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	// increment section and read CellID
	if i.nr+1 > i.indexOffset {
		return false
	}
	if nsn := i.sectionNo + 1; nsn < len(i.index) && i.index[nsn] == i.nr {
		i.cellID = 0
		i.sectionNo++
	}
	key, n := binary.Uvarint(i.buf[i.nr:])
	i.nr += n
	i.cellID += s2.CellID(key)

	// read value length
	if i.nr+1 > i.indexOffset {
		return false
	}
	vln, n := binary.Uvarint(i.buf[i.nr:])
	i.nr += n

	// read value
	if i.nr+int(vln) > i.indexOffset {
		return false
	}
	i.value = i.buf[i.nr : i.nr+int(vln)]
	i.nr += int(vln)

	return true
}

// Seek advances the cursor to the entry with CellID >= the given value.
func (i *Iterator) Seek(cellID s2.CellID) bool {
	spos := sort.Search(len(i.index), func(n int) bool {
		off := i.index[n]
		first, _ := binary.Uvarint(i.buf[off:])
		return s2.CellID(first) >= cellID
	})

	i.nr = 0
	i.cellID = 0
	i.sectionNo = spos - 1
	if spos > 0 {
		i.nr = i.index[i.sectionNo]
	}

	for i.Next() {
		if i.cellID >= cellID {
			return true
		}
	}
	return false
}

// NextBlock jumps to the next block, returns true if successful.
func (i *Iterator) NextBlock() bool {
	return i.advance(i.blockNo + 1)
}

// PrevBlock jumps to the previous block, returns true if successful.
func (i *Iterator) PrevBlock() bool {
	return i.advance(i.blockNo - 1)
}

func (i *Iterator) advance(blockNo int) bool {
	if i.err != nil {
		return false
	}

	if blockNo < 0 || blockNo >= len(i.parent.index) {
		return false
	}

	j, err := i.parent.readBlock(blockNo)
	if err != nil {
		i.err = err
		return false
	}

	i.Release()
	*i = *j
	return true
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
	releaseBuffer(i.buf)
}
