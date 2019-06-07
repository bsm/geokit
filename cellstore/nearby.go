package cellstore

import (
	"sort"
	"sync"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

var nearbySlicePool sync.Pool

// NearbyEntry is returned by Nearby search.
type NearbyEntry struct {
	s2.CellID
	Distance s1.Angle
}

func makeNearbySlice(cp int) NearbySlice {
	if v := nearbySlicePool.Get(); v != nil {
		return v.(NearbySlice)[:0]
	}
	return make(NearbySlice, 0, cp)
}

// NearbySlice is a slice of nearby entries.
type NearbySlice []NearbyEntry

func (s NearbySlice) Len() int           { return len(s) }
func (s NearbySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s NearbySlice) Less(i, j int) bool { return s[i].Distance < s[j].Distance }

// Release releases the slice.
func (s NearbySlice) Release() {
	if cap(s) != 0 {
		nearbySlicePool.Put(s)
	}
}

// Sort sorts the entries by distance.
func (s NearbySlice) Sort() { sort.Sort(s) }

func (s NearbySlice) limit(limit int) NearbySlice {
	if limit < len(s) {
		s = s[:limit]
	}
	return s
}
