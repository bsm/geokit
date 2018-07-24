package cellstore

import (
	"sort"
	"sync"

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
	bnum int // block number
	boff int // block offset
}

type nearbySlice []nearbyEntry

func (s nearbySlice) SortByDistance(origin s2.CellID) {
	sort.Slice(s, func(i, j int) bool {
		o := origin.Point()
		return o.Distance(s[i].Point()) < o.Distance(s[j].Point())
	})
}

func (s nearbySlice) Sort() {
	sort.Slice(s, func(i, j int) bool {
		return s[i].CellID < s[j].CellID
	})
}

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
