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

	eoi := len(buf) - 4
	numEntries := int(binary.LittleEndian.Uint32(buf[eoi:]))
	return &Iterator{
		parent:     r,
		blockNo:    blockNo,
		numEntries: numEntries,
		blockBuf:   buf[:eoi],
		entryPos:   -1,
	}, nil
}

type near struct {
	CellID   s2.CellID
	Value    []byte
	Distance float64
}

type nearby []near

func (n nearby) Len() int      { return len(n) }
func (n nearby) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

type byDistance struct{ nearby }

func (n byDistance) Less(i, j int) bool { return n.nearby[i].Distance < n.nearby[j].Distance }

// FindNearby
func (r *Reader) FindNearby(loc s2.CellID, limit int) (nearby, error) {

	var dst nearby

	// find the block
	it, err := r.FindBlock(loc)
	if err != nil {
		return dst, err
	}

	// iterate of the matching block
	before := 0
	after := 0
	for it.Next() {
		if it.CellID() < loc {
			before += 1
		} else {
			after += 1
		}

		dist := float64(s2.CellFromCellID(it.CellID()).Distance(loc.Point()))
		dst = append(dst, near{CellID: it.CellID(), Value: it.Value(), Distance: dist})

		// break if we have enough records after the pivot
		if after >= limit {
			break
		}
	}

	// prepend previous blocks values if required
	if before < limit && it.PrevBlock() {
		var res nearby
		for it.Next() {
			dist := float64(s2.CellFromCellID(it.CellID()).Distance(loc.Point()))
			res = append(res, near{CellID: it.CellID(), Value: it.Value(), Distance: dist})
		}
		res = res[len(res)-(limit-before):]
		dst = append(res, dst...)
	}

	// append next blocks values if required
	if after < limit && it.NextBlock() {
		for it.Next() {
			after += 1

			dist := float64(s2.CellFromCellID(it.CellID()).Distance(loc.Point()))
			dst = append(dst, near{CellID: it.CellID(), Value: it.Value(), Distance: dist})

			// break if we have enough records after the pivot
			if after >= limit {
				break
			}
		}
	}

	// sort the results
	sort.Sort(byDistance{dst})

	if len(dst) < limit {
		return dst, nil
	}

	return dst[:limit], nil
}

// --------------------------------------------------------------------

type Iterator struct {
	parent     *Reader
	blockNo    int // block number
	numEntries int // total number of entries
	blockBuf   []byte

	cursor   int // buffer cursor position
	entryPos int // current entry position

	cellID s2.CellID
	value  []byte
	err    error
}

// Next advances the cursor to the next entry
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	// read CellID
	if i.cursor+1 > len(i.blockBuf) {
		return false
	}
	key, n := binary.Uvarint(i.blockBuf[i.cursor:])
	i.cellID += s2.CellID(key)
	i.cursor += n

	// read value length
	if i.cursor+1 > len(i.blockBuf) {
		return false
	}
	vln, n := binary.Uvarint(i.blockBuf[i.cursor:])
	i.cursor += n

	// read value
	if i.cursor+int(vln) > len(i.blockBuf) {
		return false
	}
	i.value = i.blockBuf[i.cursor : i.cursor+int(vln)]
	i.cursor += int(vln)
	i.entryPos++

	return true
}

// Seek advances the cursor to the entry with CellID >= the given value.
func (i *Iterator) Seek(cellID s2.CellID) bool {
	if cellID >= i.cellID {
		for i.Next() {
			if i.cellID >= cellID {
				return true
			}
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

// Len returns the number of entries in the current block.
func (i *Iterator) Len() int {
	return i.numEntries
}

// Err returns iterator errors
func (i *Iterator) Err() error {
	return i.err
}

// Release releases the iterator. It must not be used once this method is called.
func (i *Iterator) Release() {
	releaseBuffer(i.blockBuf)
}
