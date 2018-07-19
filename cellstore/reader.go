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

// Nearby returns a limited iterator over close to cellID.
// Please note that the iterator entries are not sorted.
func (r *Reader) Nearby(origin s2.CellID, limit int) (*NearbyIterator, error) {
	block, err := r.FindBlock(origin)
	if err != nil {
		return nil, err
	}

	block.SeekSection(origin)
	originalSection := block.sectionNum
	left, right := fetchNearbySlice(2*limit), fetchNearbySlice(2*limit)

	blockOff := block.bufOff
	for block.Next() {
		if cellID := block.CellID(); cellID < origin {
			left = left.PushLeft(nearbyEntry{
				CellID: cellID,
				num:    block.blockNum,
				off:    blockOff,
			})
		} else {
			right = right.PushRight(nearbyEntry{
				CellID: cellID,
				num:    block.blockNum,
				off:    blockOff,
			})
		}
		if len(right) >= limit {
			break
		}
		blockOff = block.bufOff
	}

	for sn := originalSection - 1; sn >= 0; sn-- {
		if len(left) >= limit || !block.advanceSection(sn) {
			break
		}
		for block.Next() {
			if block.sectionNum > sn {
				break
			}
			left = left.PushLeft(nearbyEntry{
				CellID: block.CellID(),
				num:    block.blockNum,
				off:    block.bufOff,
			})
		}
	}

	entries := append(left, right...)
	entries.SortByDistance(origin)
	entries = entries.Limit(limit)
	entries.Sort()
	nearbySlicePool.Put(right)

	return &NearbyIterator{
		block:   block,
		entries: entries,
		pos:     -1,
	}, nil
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
