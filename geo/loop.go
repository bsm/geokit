package geo

import (
	"github.com/golang/geo/s2"
)

// LoopOverlap defines the type of overlap between a
// loop and a Cell
type LoopOverlap uint8

const (
	LoopOverlap_None            LoopOverlap = iota // no overlap
	LoopOverlap_Partial                            // partial overlap = intersection
	LoopOverlap_ContainsCell                       // loop completely wraps the cell
	LoopOverlap_ContainedByCell                    // cell completely wraps the loop
)

// LoopIntersectsWithCell returns the overlap relationship between
// the loop and a cell
func LoopIntersectsWithCell(loop *s2.Loop, cell s2.Cell) LoopOverlap {

	// Quick rejection test
	if !loop.RectBound().Intersects(cell.RectBound()) {
		return LoopOverlap_None
	}

	// Check of if any of the edges cross, if they do
	// we have a partial overlap
	for i := 0; i < loop.NumEdges(); i++ {
		a, b := loop.Edge(i)
		cc := s2.NewChainEdgeCrosser(a, b, cell.Vertex(3))

		for k := 0; k < 4; k++ {
			if cc.EdgeOrVertexChainCrossing(cell.Vertex(k)) {
				return LoopOverlap_Partial
			}
		}
	}

	// Check if loop contains any of the cell vertices
	if loop.ContainsPoint(cell.Vertex(0)) {
		return LoopOverlap_ContainsCell
	}

	// Check if cell contains any of the loop vertices
	if cell.ContainsPoint(loop.Vertex(0)) {
		return LoopOverlap_ContainedByCell
	}

	return LoopOverlap_None
}

// LoopIntersectionWithCell returns sub-loops which contain
// the intersection areas between the loop and a cell.
func LoopIntersectionWithCell(loop *s2.Loop, cell s2.Cell) []s2.Loop {

	// skip unless we have partial overlap
	switch LoopIntersectsWithCell(loop, cell) {
	case LoopOverlap_None:
		return nil
	case LoopOverlap_ContainsCell:
		return []s2.Loop{*s2.LoopFromCell(cell)}
	case LoopOverlap_ContainedByCell:
		return []s2.Loop{*s2.LoopFromPoints(loop.Vertices())}
	}

	// create circular linked point lists for subject and clip
	subj, clip := circularLoopFromCell(cell), circularLoopFromPoints(loop.Vertices())

	// find intersections between subject and clip
	// insert them into the loops
	subj.DoEdges(func(a, b *circularLoop) {
		crosser := s2.NewEdgeCrosser(a.Point, b.Point)

		clip.DoEdges(func(c, d *circularLoop) {
			if crosser.EdgeOrVertexCrossing(c.Point, d.Point) {
				x := EdgeIntersection(a.Point, b.Point, c.Point, d.Point)
				a.PushIntersection(x)
				c.PushIntersection(x)
			}
		})
	})

	// prepare result
	var res []s2.Loop

	// traverse paths https://codepen.io/bsm/pen/rwPQOL?editors=0010
	clip.Do(func(p *circularLoop) {
		if !p.Done && p.Intersection && !cell.ContainsPoint(p.Prev().Point) && cell.ContainsPoint(p.Next().Point) {
			pts := make([]s2.Point, 0, 4)

			c1, c2 := p, subj
			for i := 0; ; i++ {
				pts = append(pts, c1.Point)
				c1.Done = true

				if c1 = c1.Next(); c1.Intersection {
					c1, c2 = c2.Find(c1.Point), c1
				}
				if c1 == p {
					break
				}
			}

			res = append(res, *s2.LoopFromPoints(pts))
		}
	})

	return res
}

// FitLoop returns an un-normalised CellUnion approximating
// the surface covered by the loop, with the smallest
// cell being maxLevel.
func FitLoop(loop *s2.Loop, acc s2.CellUnion, maxLevel int) s2.CellUnion {
	for i := 0; i < 6; i++ {
		acc = fitLoop(loop, s2.CellIDFromFace(i), acc, maxLevel)
	}

	return acc
}

func fitLoop(loop *s2.Loop, cellID s2.CellID, acc s2.CellUnion, maxLevel int) s2.CellUnion {
	cell := s2.CellFromCellID(cellID)

	switch LoopIntersectsWithCell(loop, cell) {
	case LoopOverlap_ContainsCell:
		acc = append(acc, cellID)
	case LoopOverlap_Partial, LoopOverlap_ContainedByCell:
		if cell.Level() == maxLevel {
			acc = append(acc, cellID)
		} else {
			for _, childID := range cellID.Children() {
				acc = fitLoop(loop, childID, acc, maxLevel)
			}
		}
	}

	return acc
}
