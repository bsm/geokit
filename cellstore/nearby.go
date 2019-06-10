package cellstore

import (
	"sort"
	"sync"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

var nearbyRSPool sync.Pool

// NearbyRS is the nearby result set.
type NearbyRS struct {
	Entries []NearbyEntry
	buf     []byte
}

func newNearbyRS() *NearbyRS {
	if v := nearbyRSPool.Get(); v != nil {
		n := v.(*NearbyRS)
		n.Reset()
		return n
	}
	return new(NearbyRS)
}

// Len returns the number of entries in the set.
func (n *NearbyRS) Len() int {
	return len(n.Entries)
}

// Reset resets the set.
func (n *NearbyRS) Reset() {
	if n != nil {
		n.Entries = n.Entries[:0]
		n.buf = n.buf[:0]
	}
}

// Release releases the result set.
func (n *NearbyRS) Release() {
	if n != nil {
		nearbyRSPool.Put(n)
	}
}

func (n *NearbyRS) add(cellID s2.CellID, value []byte, distance s1.Angle) {
	off := len(n.buf)
	n.buf = append(n.buf, value...)
	n.Entries = append(n.Entries, NearbyEntry{
		CellID:   cellID,
		Value:    n.buf[off:len(n.buf)],
		Distance: distance,
	})
}

func (n *NearbyRS) sort() {
	sort.Sort(nearbyEntrySlice(n.Entries))
}

func (n *NearbyRS) limit(limit int) {
	if limit < len(n.Entries) {
		n.Entries = n.Entries[:limit]
	}
}

// NearbyEntry is returned by Nearby search.
type NearbyEntry struct {
	s2.CellID
	Value    []byte
	Distance s1.Angle
}

// NearbyEntrySlice is a slice of nearby entries.
type nearbyEntrySlice []NearbyEntry

func (s nearbyEntrySlice) Len() int           { return len(s) }
func (s nearbyEntrySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nearbyEntrySlice) Less(i, j int) bool { return s[i].Distance < s[j].Distance }
