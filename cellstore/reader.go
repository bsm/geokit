package cellstore

import (
	"io"

	"github.com/bsm/sntable"
	"github.com/golang/geo/s2"
)

// Reader represents a cellstore reader
type Reader struct {
	*sntable.Reader
}

// NewReader opens a reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	tr, err := sntable.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	return &Reader{Reader: tr}, nil
}

// FindSection finds a section right before the the cellID.
func (r *Reader) FindSection(cellID s2.CellID) (*SectionIterator, error) {
	if !cellID.IsValid() {
		return nil, errInvalidCellID
	}

	key := uint64(cellID)
	b, err := r.SeekBlock(key)
	if err != nil {
		return nil, err
	}

	s := b.SeekSection(key)
	return &SectionIterator{r: r, b: b, s: s, bpos: b.Pos(), spos: s.Pos()}, nil
}

// Nearby returns a limited iterator over close to cellID.
// Please note that the iterator entries are not sorted.
func (r *Reader) Nearby(cellID s2.CellID, limit int) (*NearbyRS, error) {
	iter, err := r.FindSection(cellID)
	if err != nil {
		return nil, err
	}
	defer iter.Release()

	numEntries := limit + 4
	rs := newNearbyRS()
	origin := cellID.Point()

	// count number of records left and right of pivot
	var nleft, nright int

ForwardLoop:
	for {
		for iter.Next() {
			cID := iter.CellID()
			rs.add(cID, iter.Value(), cID.Point().Distance(origin))
			if cID < cellID {
				nleft++
			} else if nright++; nright >= numEntries {
				break ForwardLoop
			}
		}
		if !iter.NextSection() {
			break
		}
	}

	iter.Reset()

ReverseLoop:
	for iter.PrevSection() {
		for iter.Next() {
			cID := iter.CellID()
			rs.add(cID, iter.Value(), cID.Point().Distance(origin))
			if cID >= cellID {
				nright++
			} else if nleft++; nleft >= numEntries {
				break ReverseLoop
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	rs.sort()
	rs.limit(limit)
	return rs, nil
}

// --------------------------------------------------------------------

// SectionIterator is a section iterator
type SectionIterator struct {
	r *Reader
	b *sntable.BlockReader
	s *sntable.SectionReader

	bpos int // original block position
	spos int // original section position
	err  error
}

// Release releases the iterator to the pool.
func (i *SectionIterator) Release() {
	i.b.Release()
	i.s.Release()
	i.err = errReleased
}

// Err exposes errors.
func (i *SectionIterator) Err() error { return i.err }

// CellID returns the CellID of the current entry.
func (i *SectionIterator) CellID() s2.CellID { return s2.CellID(i.s.Key()) }

// Value returns the data of the current entry.
func (i *SectionIterator) Value() []byte { return i.s.Value() }

// Next advances the cursor to the next entry in the section.
func (i *SectionIterator) Next() bool { return i.s.Next() }

// NextSection advances the iterator to the next section.
func (i *SectionIterator) NextSection() bool {
	if n := i.s.Pos() + 1; n < i.b.NumSections() {
		return i.moveTo(i.b.Pos(), n)
	} else if n := i.b.Pos() + 1; n < i.r.NumBlocks() {
		return i.moveTo(n, 0)
	}
	return false
}

// PrevSection advances the cursor to the begin of the previous section.
func (i *SectionIterator) PrevSection() bool {
	if n := i.s.Pos() - 1; n > -1 {
		return i.moveTo(i.b.Pos(), n)
	} else if n := i.b.Pos() - 1; n > -1 {
		return i.moveTo(n, -1)
	}
	return false
}

// Reset resets the position to the origin.
func (i *SectionIterator) Reset() bool {
	return i.moveTo(i.bpos, i.spos)
}

func (i *SectionIterator) moveTo(bpos, spos int) bool {
	if i.err != nil {
		return false
	}

	if i.b.Pos() != bpos {
		i.b.Release()
		i.b, i.err = i.r.GetBlock(bpos)
		return i.moveTo(bpos, spos)
	}

	if spos < 0 {
		spos = i.b.NumSections() - spos
	}

	if i.s.Pos() != spos {
		i.s.Release()
		i.s = i.b.GetSection(spos)
	}
	return true
}
