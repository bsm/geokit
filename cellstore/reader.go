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

func (r *Reader) blockOffset(blockNum int) int64 {
	if blockNum < len(r.index) {
		return r.index[blockNum].Offset
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

func (r *Reader) readBlock(blockNum int) (*Iterator, error) {
	min := r.index[blockNum].Offset
	max := r.indexOffset
	if next := blockNum + 1; next < len(r.index) {
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

	numSections := int(binary.LittleEndian.Uint32(buf[len(buf)-4:]))
	indexOffset := len(buf) - numSections*4
	index := append(make([]int, 0, numSections), 0)
	for n := indexOffset; n < len(buf)-4; n += 4 {
		index = append(index, int(binary.LittleEndian.Uint32(buf[n:])))
	}

	return &Iterator{
		parent:   r,
		blockNum: blockNum,
		index:    index,
		buf:      buf[:indexOffset],
	}, nil
}

// --------------------------------------------------------------------

type Iterator struct {
	parent     *Reader
	blockNum   int   // block number
	sectionNum int   // section number
	index      []int // section index

	buf    []byte // block buffer
	bufOff int    // number of buffer bytes read

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
	if i.bufOff+1 > len(i.buf) {
		return false
	}
	if nsn := i.sectionNum + 1; nsn < len(i.index) && i.index[nsn] == i.bufOff {
		i.cellID = 0
		i.sectionNum++
	}
	key, n := binary.Uvarint(i.buf[i.bufOff:])
	i.bufOff += n
	i.cellID += s2.CellID(key)

	// read value length
	if i.bufOff+1 > len(i.buf) {
		return false
	}
	vln, n := binary.Uvarint(i.buf[i.bufOff:])
	i.bufOff += n

	// read value
	if i.bufOff+int(vln) > len(i.buf) {
		return false
	}
	i.value = i.buf[i.bufOff : i.bufOff+int(vln)]
	i.bufOff += int(vln)

	return true
}

// SeekSection advances the cursor to the section with the first cell >= cellID.
func (i *Iterator) SeekSection(cellID s2.CellID) bool {
	pos := sort.Search(len(i.index), func(n int) bool {
		off := i.index[n]
		first, _ := binary.Uvarint(i.buf[off:])
		return s2.CellID(first) > cellID
	}) - 1

	if pos < 0 {
		pos = 0
	}
	i.cellID = 0
	i.bufOff = i.index[pos]
	i.sectionNum = pos

	return i.Next()
}

// SeekSection advances the cursor to the cell >= cellID
func (i *Iterator) Seek(cellID s2.CellID) bool {
	if !i.SeekSection(cellID) {
		return false
	}

	if i.cellID >= cellID {
		return true
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
	return i.advanceBlock(i.blockNum + 1)
}

// PrevBlock jumps to the previous block, returns true if successful.
func (i *Iterator) PrevBlock() bool {
	return i.advanceBlock(i.blockNum - 1)
}

func (i *Iterator) advanceBlock(blockNum int) bool {
	if i.err != nil {
		return false
	}

	if blockNum < 0 || blockNum >= len(i.parent.index) {
		return false
	}

	j, err := i.parent.readBlock(blockNum)
	if err != nil {
		i.err = err
		return false
	}

	i.Release()
	*i = *j
	return true
}

// firstInSection advances the cursor to the first item in secion num
func (i *Iterator) firstInSection(num int) bool {
	if num < 0 || num >= len(i.index) {
		return false
	}

	i.cellID = 0
	i.sectionNum = num
	i.bufOff = i.index[num]
	return i.Next()
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
