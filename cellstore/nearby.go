package cellstore

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/golang/geo/s2"
)

// NearbyIterator iterates across entries nearby.
type NearbyIterator struct {
	block   *Iterator
	entries []nearbyEntry
	pos     int
}

// Next advances the cursor to the next entry
// TODO: implement properly
func (i *NearbyIterator) Next() bool {
	if i.pos+1 < len(i.entries) {
		i.pos++
		return true
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

// Release releases the iterator.
func (i *NearbyIterator) Release() {
	releaseNearbySlice(i.entries)
}

// Err returns any errors from the iteration.
// TODO: implement properly
func (i *NearbyIterator) Err() error {
	return nil
}

// --------------------------------------------------------------------

type nearbyEntry struct {
	s2.CellID
	num int // block number
	off int // block offset
}

type nearbySlice []nearbyEntry

func (s nearbySlice) String() string {
	nums := make([]string, len(s))
	for i, e := range s {
		nums[i] = fmt.Sprintf("%d", e.CellID)
	}
	return strings.Join(nums, ",")
}

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

func (s nearbySlice) Shift() nearbySlice {
	if n := len(s); n > 0 {
		copy(s, s[1:])
		return s[:n-1]
	}
	return s
}

func (s nearbySlice) PushLeft(e nearbyEntry) nearbySlice {
	if len(s) == cap(s) {
		s = s.Shift()
	}
	return append(s, e)
}

func (s nearbySlice) PushRight(e nearbyEntry) nearbySlice {
	if len(s) < cap(s) {
		s = append(s, e)
	}
	return s
}

// --------------------------------------------------------------------

var nearbySlicePool sync.Pool

func fetchNearbySlice(minCap int) nearbySlice {
	if v := nearbySlicePool.Get(); v != nil {
		if p := v.(nearbySlice); minCap <= cap(p) {
			return p[:0]
		}
	}
	return make(nearbySlice, 0, minCap)
}

func releaseNearbySlice(p nearbySlice) {
	if cap(p) != 0 {
		nearbySlicePool.Put(p)
	}
}
