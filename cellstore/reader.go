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

// Nearby returns a limited iterator over close to cellID.
// Please note that the iterator entries are not sorted.
func (r *Reader) Nearby(cellID s2.CellID, limit int) (*NearbyIterator, error) {
	it, err := r.FindBlock(cellID)
	if err != nil {
		return nil, err
	}
	it.SeekSection(cellID)

	obnum, osnum := it.bnum, it.snum
	numEntries := limit + 4
	entries := fetchNearbySlice(2 * numEntries)
	origin := cellID.Point()

	// count number of records left and right of the origin
	var left, right int

	// perform a forward iteration
	it.fwd(func(cID s2.CellID, bnum, boff int) bool {
		entries = append(entries, nearbyEntry{
			CellID:   cID,
			distance: cID.Point().Distance(origin),
			bnum:     bnum,
			boff:     boff,
		})
		if cID < cellID {
			left++
		} else {
			right++
		}
		return right < numEntries
	})
	if err := it.Err(); err != nil {
		return nil, err
	}

	// perform a reverse iteration
	if left < numEntries && it.moveTo(obnum, osnum) {
		it.rev(func(cID s2.CellID, bnum, boff int, lastInSection bool) bool {
			entries = append(entries, nearbyEntry{
				CellID:   cID,
				distance: cID.Point().Distance(origin),
				bnum:     bnum,
				boff:     boff,
			})
			if cID < cellID {
				left++
			} else {
				right++
			}
			return !lastInSection || left < numEntries
		})
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	entries.SortByDistance()
	entries = entries.Limit(limit)

	return &NearbyIterator{
		block:   it,
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
		return blankIterator(r, 0), nil
	}

	blockPos := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxCellID >= cellID
	})
	if blockPos >= len(r.index) {
		return blankIterator(r, len(r.index)), nil
	}

	return r.readBlock(blockPos)
}

func (r *Reader) readBlock(bnum int) (*Iterator, error) {
	min := r.index[bnum].Offset
	max := r.indexOffset
	if next := bnum + 1; next < len(r.index) {
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

	index := append(fetchIntSlice(numSections), 0)
	for n := indexOffset; n < len(buf)-4; n += 4 {
		index = append(index, int(binary.LittleEndian.Uint32(buf[n:])))
	}

	return &Iterator{
		parent: r,
		bnum:   bnum,
		index:  index,
		buf:    buf[:indexOffset],
	}, nil
}
