package geo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/golang/geo/s2"
)

// LoopKind is the hierarchical position
// of the Loop. It is analogous to an OSM
// member role.
type LoopKind uint8

const (
	LoopKindOuter LoopKind = 1 + iota
	LoopKindInner
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

// Loop wraps s2.Loop along with its Kind.
type Loop struct {
	*s2.Loop
	Kind LoopKind
}

// Overlap returns the overlap relationship between
// the loop and a cell
func (l *Loop) Overlap(cell s2.Cell) LoopOverlap {

	// Quick rejection test
	if !l.RectBound().Intersects(cell.RectBound()) {
		return LoopOverlap_None
	}

	// Check of if any of the edges cross, if they do
	// we have a partial overlap
	for i := 0; i < l.NumEdges(); i++ {
		a, b := l.Edge(i)
		cc := s2.NewChainEdgeCrosser(a, b, cell.Vertex(3))

		for k := 0; k < 4; k++ {
			if cc.EdgeOrVertexChainCrossing(cell.Vertex(k)) {
				return LoopOverlap_Partial
			}
		}
	}

	// Check if loop contains any of the cell vertices
	if l.ContainsPoint(cell.Vertex(0)) {
		return LoopOverlap_ContainsCell
	}

	// Check if cell contains any of the loop vertices
	if cell.ContainsPoint(l.Vertex(0)) {
		return LoopOverlap_ContainedByCell
	}

	return LoopOverlap_None
}

// MarshalBinary implements encoding.BinaryMarshaler
func (l *Loop) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := l.marshalTo(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (l *Loop) UnmarshalBinary(b []byte) error {
	if len(b) < 6 {
		return io.EOF
	}

	// check version
	if ver := b[0]; ver != 1 {
		return fmt.Errorf("geo: unknown loop encoding version: %d", ver)
	}

	// check kind
	if kind := LoopKind(b[1]); kind != LoopKindInner && kind != LoopKindOuter {
		return fmt.Errorf("geo: unknown loop kind: %d", kind)
	} else {
		l.Kind = kind
	}

	// read pts len
	sz := int(binary.LittleEndian.Uint32(b[2:]))
	if len(b) != 6+(24*sz) {
		return io.EOF
	}

	// parse pts
	pts := make([]s2.Point, 0, sz)
	for i := 0; i < sz; i++ {
		x := math.Float64frombits(binary.LittleEndian.Uint64(b[6+(24*i):]))
		y := math.Float64frombits(binary.LittleEndian.Uint64(b[6+(24*i)+8:]))
		z := math.Float64frombits(binary.LittleEndian.Uint64(b[6+(24*i)+16:]))
		pts = append(pts, s2.PointFromCoords(x, y, z))
	}
	l.Loop = s2.LoopFromPoints(pts)

	return nil
}

func (l *Loop) marshalTo(buf *bytes.Buffer) error {
	// version
	if err := buf.WriteByte(uint8(1)); err != nil {
		return err
	}

	// kind
	if err := buf.WriteByte(uint8(l.Kind)); err != nil {
		return err
	}

	// vertices
	if err := binWrite(buf, uint32(len(l.Vertices()))); err != nil {
		return err
	}
	for _, pt := range l.Vertices() {
		for _, f := range []float64{pt.X, pt.Y, pt.Z} {
			if err := binWrite(buf, math.Float64bits(f)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Fit returns an un-normalised CellUnion approximating
// the surface covered by the loop, with the smallest
// cell being maxLevel.
func (l *Loop) Fit(acc s2.CellUnion, maxLevel int) s2.CellUnion {
	for i := 0; i < 6; i++ {
		acc = l.fitAppend(s2.CellIDFromFace(i), acc, maxLevel)
	}

	return acc
}

func (l *Loop) fitAppend(cellID s2.CellID, acc s2.CellUnion, maxLevel int) s2.CellUnion {
	cell := s2.CellFromCellID(cellID)

	switch l.Overlap(cell) {
	case LoopOverlap_ContainsCell:
		acc = append(acc, cellID)
	case LoopOverlap_Partial, LoopOverlap_ContainedByCell:
		if cell.Level() == maxLevel {
			acc = append(acc, cellID)
		} else {
			for _, childID := range cellID.Children() {
				acc = l.fitAppend(childID, acc, maxLevel)
			}
		}
	}

	return acc
}
