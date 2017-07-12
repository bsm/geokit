// Package osmx is a parsing extension for OpenStreetMap XML data
package osmx

import (
	"fmt"
	"math"

	"github.com/bsm/geokit/geo"
	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s2"
)

// Line denotes a Path that may or may not be closed.
type Line struct {
	Role string
	Path []*osm.Node
}

// FirstID returns the first node ID.
func (l *Line) FirstID() int64 { return l.Path[0].ID }

// LastID returns the last node ID.
func (l *Line) LastID() int64 { return l.Path[len(l.Path)-1].ID }

// IsRing denotes whether the line's Path is closed.
func (l *Line) IsRing() bool { return l.Path[0].ID == l.Path[len(l.Path)-1].ID }

// IsValid denotes whether the line is valid.
func (l *Line) IsValid() bool { return len(l.Path) != 0 && l.Role != "" }

// Merge joins together l and o if possible and returns whether
// the lines were merged.
func (l *Line) Merge(o *Line) bool {
	isChanged := true

	if l.LastID() == o.FirstID() {
		// l: a b c d
		// o: d e f g
		// l+o -> a b c d e f g
		l.Path = append(l.Path, o.Path[1:]...)
	} else if l.FirstID() == o.LastID() {
		// l: d e f g
		// o: a b c d
		// o+l -> a b c d e f g
		l.Path = append(o.Path, l.Path[1:]...)
	} else if l.FirstID() == o.FirstID() {
		// l: d c b a
		// o: d e f g
		// rev(l)+o -> a b c d e f g
		l.Path = append(l.reversePath(), o.Path[1:]...)
	} else if l.LastID() == o.LastID() {
		// l: a b c d
		// o: g f e d
		// l+rev(o) -> a b c d e f g
		l.Path = append(l.Path, o.reversePath()[1:]...)
	} else {
		isChanged = false
	}

	return isChanged
}

// Loop constructs an s2.Loop object
// from the path. It returns an empty Loop
// if the line is not closed.
func (l *Line) Loop() *s2.Loop {
	if len(l.Path) < 4 || !l.IsValid() || !l.IsRing() {
		return s2.EmptyLoop()
	}

	var pts []s2.Point
	// Discard first node as s2.Loop implicitly assumes Loops are closed.
	// First and last nodes are identical.
	for _, nd := range l.Path[1:] {
		pts = append(pts, s2.PointFromLatLng(s2.LatLngFromDegrees(nd.Lat, nd.Lng)))
	}
	// Check the direction of the points and
	// reverse the direction if clockwise.
	lp := s2.LoopFromPoints(pts)
	// The loop is assumed clockwise
	// if its bounding rectangle is over
	// 50% of the unit sphere's
	// surface area. No country exceeds this.
	// NOTE: Complex loops' orientations
	// cannot be determined with s2.RobustSign.
	if lp.RectBound().Area() >= 2*math.Pi {
		poly := s2.Polyline(pts)
		poly.Reverse()
		// Reinitiate loop for initOriginAndBound()
		lp = s2.LoopFromPoints(pts)
	}

	return lp
}

func (l *Line) reversePath() []*osm.Node {
	for i, j := 0, len(l.Path)-1; i < j; i, j = i+1, j-1 {
		l.Path[i], l.Path[j] = l.Path[j], l.Path[i]
	}

	return l.Path
}

// --------------------------------------------------

type lineMap map[int64]*Line

// Loops constructs geo.Loop objects from the lineMap.
func (m lineMap) Loops() ([]geo.Loop, error) {
	var loop []geo.Loop

	lns, err := m.lines()
	if err != nil {
		return nil, err
	}

	var g geo.Loop
	for _, ln := range lns {
		g = geo.Loop{Loop: ln.Loop()}

		if ln.Role == "outer" {
			g.Kind = geo.LoopKindOuter
		} else {
			g.Kind = geo.LoopKindInner
		}

		loop = append(loop, g)
	}

	return loop, nil
}

func (m lineMap) lines() ([]*Line, error) {
	var lns []*Line

	for id, line := range m {
		delete(m, id)

		// try build a ring if line is just a segment
		if !line.IsRing() {
			m.expand(line)
			// fail if line is still not a ring
			if !line.IsRing() {
				return nil, fmt.Errorf("osmx: cannot build ring for way #%d", id)
			}
		}

		lns = append(lns, line)
	}

	return lns, nil
}

func (m lineMap) expand(ln *Line) {
	grown := false

	for id, mapLn := range m {
		if ln.Role != mapLn.Role {
			continue
		}

		if ln.Merge(mapLn) {
			// Remove all references to this after it's merged so they aren't called during the iteration.
			delete(m, id)
			grown = true
		}
	}

	if !ln.IsRing() && grown {
		// Recursively call to join together other matching lines.
		m.expand(ln)
	}
}
