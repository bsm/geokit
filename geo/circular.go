package geo

import (
	"github.com/golang/geo/s2"
)

// circularLoop is a circular loop of points
type circularLoop struct {
	s2.Point
	Intersection bool
	Done         bool

	next, prev *circularLoop
}

func newCircularLoop(p s2.Point) *circularLoop {
	c := &circularLoop{Point: p}
	c.next = c
	c.prev = c
	return c
}

// circularLoopFromLoop creates a new circular loop from points.
func circularLoopFromPoints(pts []s2.Point) *circularLoop {
	if len(pts) == 0 {
		return circularLoopFromPoints([]s2.Point{s2.OriginPoint()})
	}

	c := newCircularLoop(pts[0])
	d := c
	for _, p := range pts[1:] {
		d = d.push(p)
	}
	return c
}

// circularLoopFromCell creates a new circular loop from a cell.
func circularLoopFromCell(cell s2.Cell) *circularLoop {
	c := newCircularLoop(cell.Vertex(0))
	d := c
	for i := 1; i < 4; i++ {
		d = d.push(cell.Vertex(i))
	}
	return c
}

// PushIntersection inserts an intersection point after this one
func (c *circularLoop) PushIntersection(p s2.Point) {
	if e := c.Next(); e.Intersection && c.Distance(e.Point) < c.Distance(p) {
		e.PushIntersection(p)
	} else {
		c.push(p).Intersection = true
	}
}

// Find finds the node by point
func (c *circularLoop) Find(p s2.Point) *circularLoop {
	for d := c; ; {
		if d.Point == p {
			return d
		}
		if d = d.Next(); d == c {
			break
		}
	}
	return nil
}

// Del removes the node from the loop
func (c *circularLoop) Del() *circularLoop {
	b := c.Prev()
	d := c.Next()

	c.prev = nil
	c.next = nil
	b.next = d
	d.prev = b

	return b
}

// Next returns the next node.
func (c *circularLoop) Next() *circularLoop { return c.next }

// Prev returns the previous node.
func (c *circularLoop) Prev() *circularLoop { return c.prev }

// Do iterates over each node of the loop
func (c *circularLoop) Do(fn func(*circularLoop)) {
	fn(c)
	for p := c.Next(); p != c; p = p.Next() {
		fn(p)
	}
}

// DoEdges iterates over each vertex edge of the loop
func (c *circularLoop) DoEdges(fn func(*circularLoop, *circularLoop)) {
	first := c.nextVertex()
	for d := first; ; {
		e := d.Next().nextVertex()
		fn(d, e)

		if d = e; d == c {
			break
		}
	}
}

// push inserts a point after this one and returns the new node
func (c *circularLoop) push(p s2.Point) *circularLoop {
	e := c.Next()
	d := &circularLoop{Point: p}
	c.next = d
	d.prev = c
	d.next = e
	e.prev = d
	return d
}

func (c *circularLoop) nextVertex() *circularLoop {
	for d := c; ; {
		if !d.Intersection {
			return d
		}

		if d = d.Next(); d == c {
			break
		}
	}
	return nil
}
