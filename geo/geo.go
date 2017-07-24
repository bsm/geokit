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
func EdgeIntersection(a, b s2.Edge) s2.Point {
	va := s2.Point{Vector: a.V0.PointCross(a.V1).Normalize()}
	vb := s2.Point{Vector: b.V0.PointCross(b.V1).Normalize()}
	x := s2.Point{Vector: va.PointCross(vb).Normalize()}

	// Make sure the intersection point is on the correct side of the sphere.
	// Since all vertices are unit length, and edges are less than 180 degrees,
	// (a + b) and (c + d) both have positive dot product with the
	// intersection point.  We use the sum of all vertices to make sure that the
	// result is unchanged when the edges are reversed or exchanged.
	if v1, v2 := a.V0.Add(a.V1.Vector), b.V0.Add(b.V1.Vector); x.Dot(v1.Add(v2)) < 0 {
		x = s2.Point{Vector: r3.Vector{X: -x.X, Y: -x.Y, Z: -x.Z}}
	}

	// The calculation above is sufficient to ensure that "x" is within
	// kIntersectionTolerance of the great circles through (a,b) and (c,d).
	// However, if these two great circles are very close to parallel, it is
	// possible that "x" does not lie between the endpoints of the given line
	// segments.  In other words, "x" might be on the great circle through
	// (a,b) but outside the range covered by (a,b).  In this case we do
	// additional clipping to ensure that it does.
	if s2.OrderedCCW(a.V0, x, a.V1, va) && s2.OrderedCCW(b.V0, x, b.V1, vb) {
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
	if s2.OrderedCCW(b.V0, a.V0, b.V1, vb) {
		findMinDist(a.V0)
	}
	if s2.OrderedCCW(b.V0, a.V1, b.V1, vb) {
		findMinDist(a.V1)
	}
	if s2.OrderedCCW(a.V0, b.V0, a.V1, va) {
		findMinDist(b.V0)
	}
	if s2.OrderedCCW(a.V0, b.V1, a.V1, va) {
		findMinDist(b.V1)
	}
	return vmin
}
