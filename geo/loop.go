package geo

import (
	"github.com/golang/geo/s2"
)

// LoopIntersectionWithCell returns sub-loops which contain
// the intersection areas between the loop and a cell.
func LoopIntersectionWithCell(loop *s2.Loop, cell s2.Cell) []*s2.Loop {

	if wrap := s2.LoopFromCell(cell); loop.ContainsCell(cell) {
		return []*s2.Loop{wrap}
	} else if wrap.Contains(loop) {
		return []*s2.Loop{loop}
	}

	if !loop.IntersectsCell(cell) {
		return nil
	}

	// create circular linked point lists for subject and clip
	subj, clip := circularLoopFromCell(cell), circularLoopFromPoints(loop.Vertices())

	// find intersections between subject and clip
	// insert them into the loops
	subj.DoEdges(func(a0, a1 *circularLoop) {
		crosser := s2.NewEdgeCrosser(a0.Point, a1.Point)

		clip.DoEdges(func(b0, b1 *circularLoop) {
			if crosser.EdgeOrVertexCrossing(b0.Point, b1.Point) {
				x := EdgeIntersection(s2.Edge{V0: a0.Point, V1: a1.Point}, s2.Edge{V0: b0.Point, V1: b1.Point})
				a0.PushIntersection(x)
				b0.PushIntersection(x)
			}
		})
	})

	// prepare result
	var res []*s2.Loop

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

			res = append(res, s2.LoopFromPoints(pts))
		}
	})

	return res
}

// FitLoop returns an un-normalised CellUnion approximating
// the surface covered by the loop, with the smallest
// cell being maxLevel.
func FitLoop(loop *s2.Loop, acc s2.CellUnion, maxLevel int) s2.CellUnion {
	FitLoopDo(loop, maxLevel, func(cellID s2.CellID) bool {
		acc = append(acc, cellID)
		return true
	})
	return acc
}

// FitLoopDo iterates over the cells of a loop and their LoopOverlap, with the smallest
// cell being maxLevel. Return false in the iterator to stop the loop.
func FitLoopDo(loop *s2.Loop, maxLevel int, fn func(s2.CellID) bool) {
	for i := 0; i < 6; i++ {
		cellID := s2.CellIDFromFace(i)
		if nxt := fitLoopDo(loop, cellID, maxLevel, fn); !nxt {
			return
		}
	}
}

func fitLoopDo(loop *s2.Loop, cellID s2.CellID, maxLevel int, fn func(s2.CellID) bool) bool {
	cell := s2.CellFromCellID(cellID)

	if loop.ContainsCell(cell) {
		return fn(cellID)
	} else if loop.IntersectsCell(cell) {
		if cell.Level() == maxLevel {
			return fn(cellID)
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
