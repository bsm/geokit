package geo

import (
	"github.com/golang/geo/s2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("circularLoop", func() {
	var subject *circularLoop

	var (
		p1 = s2.PointFromLatLng(s2.LatLngFromDegrees(52.8, -2.8))
		p2 = s2.PointFromLatLng(s2.LatLngFromDegrees(64.3, -7.7))
		p3 = s2.PointFromLatLng(s2.LatLngFromDegrees(54.2, -5.9))

		x1 = s2.Interpolate(0.3, p2, p3)
		x2 = s2.Interpolate(0.6, p2, p3)
	)

	BeforeEach(func() {
		subject = circularLoopFromPoints([]s2.Point{p1, p2, p3})
	})

	var sliceOf = func(c *circularLoop) (pts []testPointWithStatus) {
		c.Do(func(x *circularLoop) {
			pts = append(pts, testPointWithStatus{x.Point, x.Intersection})
		})
		return pts
	}

	It("should create loops from points", func() {
		pts := sliceOf(subject)
		Expect(pts).To(Equal([]testPointWithStatus{
			{p1, false},
			{p2, false},
			{p3, false},
		}))
	})

	It("should create loops from cells", func() {
		cell := s2.CellFromCellID(s2.CellIDFromToken("aa"))
		pts := sliceOf(circularLoopFromCell(cell))
		Expect(pts).To(Equal([]testPointWithStatus{
			{cell.Vertex(0), false},
			{cell.Vertex(1), false},
			{cell.Vertex(2), false},
			{cell.Vertex(3), false},
		}))
	})

	It("should iterate over edges", func() {
		var pts []s2.Point
		subject.Next().PushIntersection(x1)
		subject.DoEdges(func(a, b *circularLoop) {
			pts = append(pts, a.Point, b.Point)
		})
		Expect(pts).To(Equal([]s2.Point{
			p1, p2,
			p2, p3,
			p3, p1,
		}))
	})

	It("should traverse", func() {
		Expect(subject.Next().Point).To(Equal(p2))
		Expect(subject.Next().Next().Point).To(Equal(p3))
		Expect(subject.Prev().Point).To(Equal(p3))
	})

	It("should delete", func() {
		subject = subject.Del()
		Expect(sliceOf(subject)).To(Equal([]testPointWithStatus{
			{p3, false},
			{p2, false},
		}))

		subject = subject.Del()
		Expect(sliceOf(subject)).To(Equal([]testPointWithStatus{
			{p2, false},
		}))

		subject = subject.Del()
		Expect(sliceOf(subject)).To(Equal([]testPointWithStatus{
			{p2, false},
		}))
	})

	It("should push intersections", func() {
		b := subject.Next()

		b.PushIntersection(x1)
		Expect(sliceOf(subject)).To(Equal([]testPointWithStatus{
			{p1, false},
			{p2, false},
			{x1, true},
			{p3, false},
		}))

		b.PushIntersection(x2)
		Expect(sliceOf(subject)).To(Equal([]testPointWithStatus{
			{p1, false},
			{p2, false},
			{x1, true},
			{x2, true},
			{p3, false},
		}))
	})

})

type testPointWithStatus struct {
	s2.Point
	Intersection bool
}
