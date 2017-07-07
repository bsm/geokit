package geo

import "github.com/golang/geo/s2"

// Polygon contains the set of inner and outer s2.Loop objects.
// These are analogous to multipolygons in OSM.
// The total space enclosed by Polygon is taken
// by removing the union of the Outer loops
// from the union of the Inner loops.
// TODO: Reconsider this once geo/s2
// supports multiple Loops for Polygon
type Polygon []Loop

// Cells returns all cells that approximately fit p.
func (p Polygon) Cells(maxLevel int) s2.CellUnion {
	var acc s2.CellUnion
	for _, lp := range p {
		// TODO: Only outers are implemented
		if lp.Kind != LoopKindOuter {
			continue
		}

		acc = lp.Fit(acc, maxLevel)
	}

	acc.Normalize()
	return acc
}
