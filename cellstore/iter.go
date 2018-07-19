package cellstore

import (
	"encoding/binary"
	"sort"

	"github.com/golang/geo/s2"
)

// Iterator is a block iterator returned by the Reader
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
	return true
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

func (i *Iterator) advanceSection(num int) bool {
	if num < 0 || num >= len(i.index) {
		return false
	}

	i.cellID = 0
	i.sectionNum = num
	i.bufOff = i.index[num]
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
