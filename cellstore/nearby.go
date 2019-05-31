package cellstore

import (
	"sort"
	"sync"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// NearbyIterator iterates across entries nearby.
type NearbyIterator struct {
	block   *Iterator
	entries nearbySlice

	pos int
}

// Next advances the cursor to the next entry
func (i *NearbyIterator) Next() bool {
	if np := i.pos + 1; np < len(i.entries) {
		ent := i.entries[np]
		if ent.bnum != i.block.bnum && !i.block.toBlock(ent.bnum) {
			return false
		}
		i.pos = np
		i.block.setOffset(ent.boff)
		return i.block.Next()
	}
	return false
}

// CellID returns the cell ID at the current cursor position.
func (i *NearbyIterator) CellID() s2.CellID {
	if i.pos < len(i.entries) {
		return i.entries[i.pos].CellID
	}
	return 0
}

// Distance returns the distance to the origin at the current cursor position.
func (i *NearbyIterator) Distance() s1.Angle {
	if i.pos < len(i.entries) {
		return i.entries[i.pos].distance
	}
	return s1.InfAngle()
}

// Value returns the value at the current cursor position.
func (i *NearbyIterator) Value() []byte {
	return i.block.Value()
}

// Release releases the iterator.
func (i *NearbyIterator) Release() {
	releaseNearbySlice(i.entries)
	i.block.Release()
}

// Err returns any errors from the iteration.
func (i *NearbyIterator) Err() error {
	return i.block.Err()
}

// --------------------------------------------------------------------

type nearbyEntry struct {
	s2.CellID
	bnum     int // block number
	boff     int // block offset
	distance s1.Angle
}

type nearbySlice []nearbyEntry

func (s nearbySlice) Len() int           { return len(s) }
func (s nearbySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nearbySlice) Less(i, j int) bool { return s[i].distance < s[j].distance }
func (s nearbySlice) SortByDistance()    { sort.Sort(s) }
func (s nearbySlice) Limit(limit int) nearbySlice {
	if limit < len(s) {
		s = s[:limit]
	}
	return s
}

// --------------------------------------------------------------------

var nearbySlicePool sync.Pool

func fetchNearbySlice(cp int) nearbySlice {
	if v := nearbySlicePool.Get(); v != nil {
		return v.(nearbySlice)[:0]
	}
	return make(nearbySlice, 0, cp)
}

func releaseNearbySlice(p nearbySlice) {
	if cap(p) != 0 {
		nearbySlicePool.Put(p)
	}
}
