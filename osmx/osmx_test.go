package osmx

import (
	"testing"

	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

var _ = Describe("Line", func() {
	var subject *Line

	It("should indicate closed line", func() {
		subject = &Line{Path: mockNodes(1, 2, 3, 2, 1)}
		Expect(subject.IsRing()).To(BeTrue())
	})

	It("should indicate open line", func() {
		subject = &Line{Path: mockNodes(1, 2, 3)}
		Expect(subject.IsRing()).To(BeFalse())
	})

	It("should validate line", func() {
		subject = &Line{Role: "inner", Path: mockNodes(1, 2, 3)}
		Expect(subject.IsValid()).To(BeTrue())
	})

	It("should not validate lines without Role", func() {
		subject = &Line{Path: mockNodes(1, 2, 3)}
		Expect(subject.IsValid()).To(BeFalse())
	})

	It("should not validate lines without Path", func() {
		subject = &Line{Role: "inner"}
		Expect(subject.IsValid()).To(BeFalse())
	})

	It("should construct s2.Loop", func() {
		subject = &Line{
			Role: "outer",
			// Colorado, USA: nw, ne, se, sw, nw
			Path: []*osm.Node{
				{Lat: 41, Lng: -109},
				{Lat: 41, Lng: -102},
				{Lat: 37, Lng: -102},
				{Lat: 37, Lng: -109},
				{Lat: 41, Lng: -109},
			},
		}

		loop := subject.Loop()
		Expect(loop.NumEdges()).To(Equal(4))

		pts := loop.Vertices()
		// Colorado, USA: nw, sw, se, ne
		Expect(pts[0].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(41, -109)))).To(BeTrue())
		Expect(pts[1].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(37, -109)))).To(BeTrue())
		Expect(pts[2].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(37, -102)))).To(BeTrue())
		Expect(pts[3].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(41, -102)))).To(BeTrue())
	})

})

var _ = Describe("lineMap", func() {
	var subject lineMap

	DescribeTable("expand lines single role",
		func(lm lineMap, exp ...types.GomegaMatcher) {
			subject = lm
			lns, err := subject.lines()
			Expect(err).NotTo(HaveOccurred())
			Expect(lns).To(SatisfyAny(HaveLen(1), HaveLen(2)))

			// Iterating through a map is non-deterministic,
			// so we can start with:
			// a b c d, d e f a
			// but end up one of:
			// a b c d e f a, d e f a b c d
			// Therefore we must satisfy at least one
			// of the permutations.
			for _, ln := range lns {
				if ln.Role == "outer" {
					Expect(ln.Path).To(SatisfyAny(exp...))
				}
			}
		},
		Entry("closed way",
			lineMap{1: &Line{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)}},
			Equal(mockNodes(1, 2, 3, 2, 1)),
		),
		Entry("mixed roles, closed ways",
			lineMap{
				1: &Line{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)},
				2: &Line{Role: "inner", Path: mockNodes(1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1)},
			},
			Equal(mockNodes(1, 2, 3, 2, 1)),
		),
		Entry("simple",
			lineMap{
				1: &Line{Role: "outer", Path: mockNodes(1, 2, 3, 4, 5)},
				2: &Line{Role: "outer", Path: mockNodes(5, 4, 3, 2, 1)},
			},
			Equal(mockNodes(1, 2, 3, 4, 5, 4, 3, 2, 1)),
			Equal(mockNodes(5, 4, 3, 2, 1, 2, 3, 4, 5)),
		),
		Entry("complicated",
			lineMap{
				1: &Line{Role: "outer", Path: mockNodes(1, 2, 3)},
				2: &Line{Role: "outer", Path: mockNodes(5, 4, 3)},
				3: &Line{Role: "outer", Path: mockNodes(5, 6, 7)},
				4: &Line{Role: "outer", Path: mockNodes(1, 8, 7)},
			},
			Equal(mockNodes(1, 2, 3, 4, 5, 6, 7, 8, 1)),
			Equal(mockNodes(1, 8, 7, 6, 5, 4, 3, 2, 1)),
			Equal(mockNodes(3, 2, 1, 8, 7, 6, 5, 4, 3)),
			Equal(mockNodes(3, 4, 5, 6, 7, 8, 1, 2, 3)),
			Equal(mockNodes(5, 4, 3, 2, 1, 8, 7, 6, 5)),
			Equal(mockNodes(5, 6, 7, 8, 1, 2, 3, 4, 5)),
			Equal(mockNodes(7, 6, 5, 4, 3, 2, 1, 8, 7)),
			Equal(mockNodes(7, 8, 1, 2, 3, 4, 5, 6, 7)),
		),
		Entry("mixed roles, complicated",
			lineMap{
				1: &Line{Role: "outer", Path: mockNodes(1, 2, 3)},
				2: &Line{Role: "inner", Path: mockNodes(1, 2, 3)},
				3: &Line{Role: "outer", Path: mockNodes(5, 4, 3)},
				4: &Line{Role: "inner", Path: mockNodes(5, 4, 3)},
				5: &Line{Role: "outer", Path: mockNodes(5, 6, 7)},
				6: &Line{Role: "inner", Path: mockNodes(5, 6, 7)},
				7: &Line{Role: "outer", Path: mockNodes(1, 8, 7)},
				8: &Line{Role: "inner", Path: mockNodes(1, 8, 7)},
			},
			Equal(mockNodes(1, 2, 3, 4, 5, 6, 7, 8, 1)),
			Equal(mockNodes(1, 8, 7, 6, 5, 4, 3, 2, 1)),
			Equal(mockNodes(3, 2, 1, 8, 7, 6, 5, 4, 3)),
			Equal(mockNodes(3, 4, 5, 6, 7, 8, 1, 2, 3)),
			Equal(mockNodes(5, 4, 3, 2, 1, 8, 7, 6, 5)),
			Equal(mockNodes(5, 6, 7, 8, 1, 2, 3, 4, 5)),
			Equal(mockNodes(7, 6, 5, 4, 3, 2, 1, 8, 7)),
			Equal(mockNodes(7, 8, 1, 2, 3, 4, 5, 6, 7)),
		),
	)

	It("should ensure paths are closed", func() {
		subject = lineMap{
			1: &Line{Role: "outer", Path: mockNodes(1, 2, 3)},
		}
		_, err := subject.lines()
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(`osmx: cannot build ring for way #1`))
	})
})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/osmx")
}

// --------------------------------------------------------------------

func mockNodes(nodeIDs ...int64) []*osm.Node {
	nodes := make([]*osm.Node, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		nodes = append(nodes, &osm.Node{Elem: osm.Elem{ID: n}})
	}

	return nodes
}
