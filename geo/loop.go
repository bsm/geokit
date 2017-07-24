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
		edge := loop.Edge(i)
		cc := s2.NewChainEdgeCrosser(edge.V0, edge.V1, cell.Vertex(3))

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
	subj.DoEdges(func(a0, a1 *circularLoop) {
		crosser := s2.NewEdgeCrosser(a0.Point, a1.Point)

		clip.DoEdges(func(b0, b1 *circularLoop) {
			if crosser.EdgeOrVertexCrossing(b0.Point, b1.Point) {
				x := EdgeIntersection(s2.Edge{a0.Point, a1.Point}, s2.Edge{b0.Point, b1.Point})
				a0.PushIntersection(x)
				b0.PushIntersection(x)
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
	FitLoopDo(loop, maxLevel, func(cellID s2.CellID, _ LoopOverlap) bool {
		acc = append(acc, cellID)
		return true
	})
	return acc
}

// FitLoopDo iterates over the cells of a loop and their LoopOverlap, with the smallest
// cell being maxLevel. Return false in the iterator to stop the loop.
func FitLoopDo(loop *s2.Loop, maxLevel int, fn func(s2.CellID, LoopOverlap) bool) {
	for i := 0; i < 6; i++ {
		if nxt := fitLoopDo(loop, s2.CellIDFromFace(i), maxLevel, fn); !nxt {
			return
		}
	}
}

func fitLoopDo(loop *s2.Loop, cellID s2.CellID, maxLevel int, fn func(s2.CellID, LoopOverlap) bool) bool {
	cell := s2.CellFromCellID(cellID)
	over := LoopIntersectsWithCell(loop, cell)

	switch over {
	case LoopOverlap_ContainsCell:
		return fn(cellID, over)
	case LoopOverlap_Partial, LoopOverlap_ContainedByCell:
		if cell.Level() == maxLevel {
			return fn(cellID, over)
		} else {
			for _, childID := range cellID.Children() {
				if !fitLoopDo(loop, childID, maxLevel, fn) {
					return false
				}
			}
		}
	}
	return true
}
