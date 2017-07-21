package geo

import (
	"encoding/binary"
	"io"

	"github.com/golang/geo/r3"
	"github.com/golang/geo/s2"
)

func binWrite(w io.Writer, v interface{}) error {
	return binary.Write(w, binary.LittleEndian, v)
}

// EdgeIntersection returns the intersection point between the edges (a-b)
// and (c-d).
func EdgeIntersection(a, b, c, d s2.Point) s2.Point {
	ab := s2.Point{Vector: a.PointCross(b).Normalize()}
	cd := s2.Point{Vector: c.PointCross(d).Normalize()}
	x := s2.Point{Vector: ab.PointCross(cd).Normalize()}

	// Make sure the intersection point is on the correct side of the sphere.
	// Since all vertices are unit length, and edges are less than 180 degrees,
	// (a + b) and (c + d) both have positive dot product with the
	// intersection point.  We use the sum of all vertices to make sure that the
	// result is unchanged when the edges are reversed or exchanged.
	if v1, v2 := a.Add(b.Vector), c.Add(d.Vector); x.Dot(v1.Add(v2)) < 0 {
		x = s2.Point{Vector: r3.Vector{X: -x.X, Y: -x.Y, Z: -x.Z}}
	}

	// The calculation above is sufficient to ensure that "x" is within
	// kIntersectionTolerance of the great circles through (a,b) and (c,d).
	// However, if these two great circles are very close to parallel, it is
	// possible that "x" does not lie between the endpoints of the given line
	// segments.  In other words, "x" might be on the great circle through
	// (a,b) but outside the range covered by (a,b).  In this case we do
	// additional clipping to ensure that it does.
	if s2.OrderedCCW(a, x, b, ab) && s2.OrderedCCW(c, x, d, cd) {
		return x
	}

	// Find the acceptable endpoint closest to x and return it.  An endpoint is
	// acceptable if it lies between the endpoints of the other line segment.
	dmin2, vmin := 10.0, x
	findMinDist := func(y s2.Point) {
		d2 := x.Sub(y.Vector).Norm2()
		if d2 < dmin2 || (d2 == dmin2 && y.Cmp(vmin.Vector) == -1) {
			dmin2, vmin = d2, y
		}
	}
	if s2.OrderedCCW(c, a, d, cd) {
		findMinDist(a)
	}
	if s2.OrderedCCW(c, b, d, cd) {
		findMinDist(b)
	}
	if s2.OrderedCCW(a, c, b, ab) {
		findMinDist(c)
	}
	if s2.OrderedCCW(a, d, b, ab) {
		findMinDist(d)
	}
	return vmin
}
