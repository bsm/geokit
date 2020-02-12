// Package osmx is a parsing extension for OpenStreetMap XML data
package osmx

import (
	"fmt"
	"math"

	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

// wayPath denotes a chain of Nodes that may or may not be closed.
type wayPath struct {
	Role string
	Path []*osm.Node
}

// First returns the first node.
func (w *wayPath) First() *osm.Node { return w.Path[0] }

// Last returns the last node.
func (w *wayPath) Last() *osm.Node { return w.Path[len(w.Path)-1] }

// FirstID returns the first node ID.
func (w *wayPath) FirstID() int64 { return w.First().ID }

// LastID returns the last node ID.
func (w *wayPath) LastID() int64 { return w.Last().ID }

// IsClosed denotes whether the Path is closed.
func (w *wayPath) IsClosed() bool { return w.FirstID() == w.LastID() }

// IsValid denotes whether the way is valid.
func (w *wayPath) IsValid() bool { return len(w.Path) > 1 && (w.Role == "outer" || w.Role == "inner") }

// EdgeMerge merges o onto w if both ways have a common edge. It returns true
// if the merge was successful.
func (w *wayPath) EdgeMerge(o *wayPath) bool {
	if w.Role != o.Role {
		return false
	}

	merged := true
	if w.LastID() == o.FirstID() {
		// w: a b c d
		// o: d e f g
		// w+o -> a b c d e f g
		w.Path = append(w.Path, o.Path[1:]...)
	} else if w.FirstID() == o.LastID() {
		// w: d e f g
		// o: a b c d
		// o+w -> a b c d e f g
		w.Path = append(o.Path, w.Path[1:]...)
	} else if w.FirstID() == o.FirstID() {
		// w: d c b a
		// o: d e f g
		// rev(w)+o -> a b c d e f g
		w.Path = append(w.reversePath(), o.Path[1:]...)
	} else if w.LastID() == o.LastID() {
		// w: a b c d
		// o: g f e d
		// w+rev(w) -> a b c d e f g
		w.Path = append(w.Path, o.reversePath()[1:]...)
	} else {
		merged = false
	}

	return merged
}

// Loop constructs an s2.Loop object
// from the path. It returns an empty Loop
// if the line is not closed.
func (w *wayPath) Loop() (*s2.Loop, error) {
	if !w.IsValid() {
		return nil, fmt.Errorf("osmx: cannot build loop from an invalid way")
	} else if !w.IsClosed() {
		return nil, fmt.Errorf("osmx: cannot build loop from an open way")
	}

	var pts []s2.Point
	// Discard first node as s2.Loop implicitly assumes Loops are closed.
	// First and last nodes are identicaw.
	for _, nd := range w.Path[1:] {
		pts = append(pts, s2.PointFromLatLng(s2.LatLngFromDegrees(nd.Lat, nd.Lng)))
	}
	// Check the direction of the points and
	// reverse the direction if clockwise.
	loop := s2.LoopFromPoints(pts)
	// The loop is assumed clockwise
	// if its bounding rectangle is over
	// 50% of the unit sphere's
	// surface area. No country exceeds this.
	// NOTE: Complex loops' orientations
	// cannot be determined with s2.RobustSign.
	if loop.RectBound().Area() >= 2*math.Pi {
		poly := s2.Polyline(pts)
		poly.Reverse()
		// Reinitiate loop for initOriginAndBound()
		loop = s2.LoopFromPoints(pts)
	}

	return loop, nil
}

// ForceMerge merges o onto w using one of the following modes. Example:
//
//   w: a b c
//   o: d e f
//
//   w.ForceMerge(o, 1) => a b c d e f
//   w.ForceMerge(o, 2) => d e f a b c
//   w.ForceMerge(o, 3) => c b a d e f
//   w.ForceMerge(o, 4) => a b c f e d
func (w *wayPath) ForceMerge(o *wayPath, mode int) {
	if w.Role != o.Role {
		return
	}

	switch mode {
	case 1:
		w.Path = append(w.Path, o.Path...)
	case 2:
		w.Path = append(o.Path, w.Path...)
	case 3:
		w.Path = append(w.reversePath(), o.Path...)
	case 4:
		w.Path = append(w.Path, o.reversePath()...)
	}
}

// MinEdgeDistance returns the minimum distance between the edges of two ways.
func (w *wayPath) MinEdgeDistance(o *wayPath) (min s1.Angle, mode int) {
	min = s1.InfAngle()
	if w.Role != o.Role {
		return min, mode
	}

	w1 := s2.PointFromLatLng(s2.LatLngFromDegrees(w.First().Lat, w.First().Lng))
	w2 := s2.PointFromLatLng(s2.LatLngFromDegrees(w.Last().Lat, w.Last().Lng))

	o1 := s2.PointFromLatLng(s2.LatLngFromDegrees(o.First().Lat, o.First().Lng))
	o2 := s2.PointFromLatLng(s2.LatLngFromDegrees(o.Last().Lat, o.Last().Lng))

	if d := w2.Distance(o1); d < min {
		min, mode = d, 1
	}
	if d := w1.Distance(o2); d < min {
		min, mode = d, 2
	}
	if d := w1.Distance(o1); d < min {
		min, mode = d, 3
	}
	if d := w2.Distance(o2); d < min {
		min, mode = d, 4
	}
	return min, mode
}

// Reverse node order.
func (w *wayPath) reversePath() []*osm.Node {
	for i, j := 0, len(w.Path)-1; i < j; i, j = i+1, j-1 {
		w.Path[i], w.Path[j] = w.Path[j], w.Path[i]
	}

	return w.Path
}

// --------------------------------------------------------------------

type waySlice []*wayPath

// Reduce reduces ways to continuous loops by joining them together. Destructive!
func (s waySlice) Reduce() waySlice {
	// find simple loops by merging edges
	for i, w := range s {
		if w != nil {
			s.mergeEdges(w, i+1)
		}
	}
	s = s.compact(false)

	// open loops
	for i, w := range s {
		if w != nil && !w.IsClosed() {
			s.mergeOpen(w, i+1)
		}
	}
	return s.compact(true)
}

// removes all nils
func (s waySlice) compact(close bool) waySlice {
	clean := s[:0]
	for _, w := range s {
		if w != nil {
			if close && !w.IsClosed() {
				w.Path = append(w.Path, w.Path[0])
			}
			clean = append(clean, w)
		}
	}
	return clean
}

func (s waySlice) mergeEdges(w *wayPath, off int) {
	merged := false
	for i, x := range s[off:] {
		if x == nil {
			continue
		}

		if w.EdgeMerge(x) {
			s[i+off] = nil
			merged = true
		}
	}

	if merged {
		s.mergeEdges(w, off)
	}
}

func (s waySlice) mergeOpen(w *wayPath, off int) {
	var (
		pos      = -1
		mode     = 0
		distance = s1.InfAngle()
	)

	for i, x := range s[off:] {
		if x == nil {
			continue
		}

		if !x.IsClosed() {
			if d, m := w.MinEdgeDistance(x); d < distance {
				distance = d
				mode = m
				pos = i
			}
		}
	}

	if pos > -1 {
		w.ForceMerge(s[off+pos], mode)
		s[off+pos] = nil
		s.mergeOpen(w, off)
	}
}
