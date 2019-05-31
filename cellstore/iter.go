package cellstore

import (
	"encoding/binary"
	"sort"

	"github.com/golang/geo/s2"
)

// Iterator is a block iterator returned by the Reader
type Iterator struct {
	parent *Reader

	buf  []byte // block buffer
	bnum int    // block number
	boff int    // block offset

	index []int // section index
	snum  int   // section number

	cellID s2.CellID
	value  []byte
	err    error
}

func blankIterator(parent *Reader, bnum int) *Iterator {
	return &Iterator{parent: parent, bnum: bnum}
}

// Next reads the next entry and advances the cursor.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	// increment section and read CellID
	if i.boff+1 > len(i.buf) {
		return false
	}
	if nsn := i.snum + 1; nsn < len(i.index) && i.index[nsn] == i.boff {
		i.cellID = 0
		i.snum++
	}
	key, n := binary.Uvarint(i.buf[i.boff:])
	i.boff += n
	i.cellID += s2.CellID(key)

	// read value length
	if i.boff+1 > len(i.buf) {
		return false
	}
	vln, n := binary.Uvarint(i.buf[i.boff:])
	i.boff += n

	// read value
	if i.boff+int(vln) > len(i.buf) {
		return false
	}
	i.value = i.buf[i.boff : i.boff+int(vln)]
	i.boff += int(vln)

	return true
}

// SeekSection positions the cursor at the section with the first cell >= cellID within the current block.
func (i *Iterator) SeekSection(cellID s2.CellID) {
	pos := sort.Search(len(i.index), func(n int) bool {
		off := i.index[n]
		first, _ := binary.Uvarint(i.buf[off:])
		return s2.CellID(first) > cellID
	}) - 1

	if pos < 0 {
		pos = 0
	}
	i.cellID = 0
	i.snum = pos
	if pos < len(i.index) {
		i.boff = i.index[pos]
	}
}

// Seek positions the cursor to the cell >= cellID within the current block.
func (i *Iterator) Seek(cellID s2.CellID) {
	i.SeekSection(cellID)

	xcell, xsnum, xboff := i.cellID, i.snum, i.boff
	for i.Next() {
		if i.cellID >= cellID {
			i.cellID, i.snum, i.boff = xcell, xsnum, xboff
			return
		}
		xcell, xsnum, xboff = i.cellID, i.snum, i.boff
	}
}

// NextBlock jumps to the next block, returns true if successful.
func (i *Iterator) NextBlock() bool {
	return i.toBlock(i.bnum + 1)
}

// PrevBlock jumps to the previous block, returns true if successful.
func (i *Iterator) PrevBlock() bool {
	return i.toBlock(i.bnum - 1)
}

func (i *Iterator) fwd(fn func(cellID s2.CellID, bnum, boff int) bool) {
	if i.err != nil {
		return
	}

	var stop bool
	for {
		boff := i.boff
		for !stop && i.err == nil && i.Next() {
			stop = !fn(i.CellID(), i.bnum, boff)
			boff = i.boff
		}

		if stop || i.err != nil || !i.NextBlock() {
			break
		}
	}
}

func (i *Iterator) rev(fn func(cellID s2.CellID, bnum, boff int, lastInSection bool) bool) {
	if i.err != nil {
		return
	}

	var stop bool

	finish := i.boff
	for {
		for sn := i.snum; !stop && i.err == nil && sn >= 0; sn-- {
			if !i.toSection(sn) {
				break
			}

			if sn+1 < len(i.index) && i.index[sn+1] < finish {
				finish = i.index[sn+1]
			}

			boff := i.boff
			for boff < finish && !stop && i.err == nil && i.Next() {
				stop = !fn(i.CellID(), i.bnum, boff, i.boff == finish)
				boff = i.boff
			}
		}

		if stop || i.err != nil || !i.PrevBlock() {
			break
		}

		finish = len(i.buf)
		if n := len(i.index) - 1; n > -1 {
			i.snum = n
		}
	}
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
	releaseIntSlice(i.index)
}

func (i *Iterator) setOffset(boff int) {
	if i.boff == boff {
		return
	}

	if boff < len(i.buf) {
		i.boff = boff
	}
	i.snum = sort.Search(len(i.index), func(n int) bool {
		return i.index[n] > boff
	}) - 1

}

func (i *Iterator) moveTo(bnum, snum int) bool {
	if i.bnum != bnum && !i.toBlock(bnum) {
		return false
	}
	if i.snum != snum && !i.toSection(snum) {
		return false
	}
	return true
}

func (i *Iterator) toBlock(bnum int) bool {
	if i.err != nil {
		return false
	}

	if bnum < 0 || bnum >= len(i.parent.index) {
		return false
	}

	if bnum == i.bnum {
		i.cellID = 0
		i.snum = 0
		i.boff = 0
	}

	j, err := i.parent.readBlock(bnum)
	if err != nil {
		i.err = err
		return false
	}

	i.Release()
	*i = *j
	return true
}

func (i *Iterator) toSection(snum int) bool {
	if snum < 0 || snum >= len(i.index) {
		return false
	}

	i.cellID = 0
	i.snum = snum
	if snum < len(i.index) {
		i.boff = i.index[snum]
	}
	return true
}
