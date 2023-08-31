package osmx

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s2"
)

var _ = Describe("wayPath", func() {
	var subject *wayPath
	var northWest, northEast, southEast, southWest *osm.Node

	BeforeEach(func() {
		northWest = mockNode(1, 41, -109)
		northEast = mockNode(2, 41, -102)
		southEast = mockNode(3, 37, -102)
		southWest = mockNode(4, 37, -109)

		// Colorado, USA: nw, ne, se, sw, nw
		subject = &wayPath{
			Role: "outer",
			Path: []*osm.Node{northWest, northEast, southEast, southWest, northWest},
		}
	})

	It("should indicate closed", func() {
		subject = &wayPath{Path: mockNodes(1, 2, 3, 2, 1)}
		Expect(subject.IsClosed()).To(BeTrue())

		subject = &wayPath{Path: mockNodes(1, 2, 3)}
		Expect(subject.IsClosed()).To(BeFalse())
	})

	It("should validate", func() {
		subject = &wayPath{Role: "inner", Path: mockNodes(1, 2, 3)}
		Expect(subject.IsValid()).To(BeTrue())
	})

	It("should not validate without Role", func() {
		subject = &wayPath{Path: mockNodes(1, 2, 3)}
		Expect(subject.IsValid()).To(BeFalse())
	})

	It("should not validate without Path", func() {
		subject = &wayPath{Role: "inner"}
		Expect(subject.IsValid()).To(BeFalse())
	})

	It("should refuse open loops", func() {
		subject.Path = subject.Path[:3]
		_, err := subject.Loop()
		Expect(err).To(MatchError(`osmx: cannot build loop from an open way`))
	})

	It("should construct s2.Loop", func() {
		loop, err := subject.Loop()
		Expect(err).NotTo(HaveOccurred())
		Expect(loop.NumEdges()).To(Equal(4))

		pts := loop.Vertices()
		// Colorado, USA: nw, sw, se, ne
		Expect(pts[0].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(41, -109)))).To(BeTrue())
		Expect(pts[1].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(37, -109)))).To(BeTrue())
		Expect(pts[2].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(37, -102)))).To(BeTrue())
		Expect(pts[3].ApproxEqual(s2.PointFromLatLng(s2.LatLngFromDegrees(41, -102)))).To(BeTrue())

		Expect(loop.ContainsOrigin()).To(BeFalse())
	})

	It("should construct orientation-invariant loop", func() {
		// Colorado, USA: nw, ne, se, sw, nw
		antiClockwise := &wayPath{
			Role: "outer",
			Path: []*osm.Node{northEast, northWest, southWest, southEast, northEast},
		}

		std, err := subject.Loop()
		Expect(err).NotTo(HaveOccurred())
		Expect(antiClockwise.Loop()).To(Equal(std))
	})
})

var _ = DescribeTable("waySlice",
	func(src waySlice, exp waySlice) {
		Expect(src.Reduce()).To(Equal(exp))
	},

	Entry("closed way",
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)},
		},
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)},
		},
	),
	Entry("mixed roles, closed ways",
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)},
			{Role: "inner", Path: mockNodes(1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1)},
		},
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 2, 1)},
			{Role: "inner", Path: mockNodes(1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1)},
		},
	),
	Entry("simple merge",
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 4, 5)},
			{Role: "outer", Path: mockNodes(5, 6, 7, 2, 1)},
		},
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3, 4, 5, 6, 7, 2, 1)},
		},
	),
	Entry("complicated",
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3)},
			{Role: "outer", Path: mockNodes(5, 4, 3)},
			{Role: "outer", Path: mockNodes(5, 6, 7)},
			{Role: "outer", Path: mockNodes(1, 8, 7)},
		},
		waySlice{
			{Role: "outer", Path: mockNodes(7, 6, 5, 4, 3, 2, 1, 8, 7)},
		},
	),
	Entry("mixed roles, complicated",
		waySlice{
			{Role: "outer", Path: mockNodes(1, 2, 3)},
			{Role: "inner", Path: mockNodes(11, 12, 13)},
			{Role: "outer", Path: mockNodes(5, 4, 3)},
			{Role: "inner", Path: mockNodes(15, 14, 13)},
			{Role: "outer", Path: mockNodes(5, 6, 7)},
			{Role: "inner", Path: mockNodes(15, 16, 17)},
			{Role: "outer", Path: mockNodes(1, 8, 7)},
			{Role: "inner", Path: mockNodes(11, 18, 17)},
		},
		waySlice{
			{Role: "outer", Path: mockNodes(7, 6, 5, 4, 3, 2, 1, 8, 7)},
			{Role: "inner", Path: mockNodes(17, 16, 15, 14, 13, 12, 11, 18, 17)},
		},
	),

	Entry("unconnected",
		waySlice{
			{Role: "outer", Path: []*osm.Node{mockNode(11, 45.6, 36.6), mockNode(12, 48.9, 37.3), mockNode(13, 46.1, 42.8), mockNode(14, 44.9, 36.6)}},
			{Role: "outer", Path: []*osm.Node{mockNode(21, 45.7, 35.2), mockNode(22, 45.8, 32.9)}},
			{Role: "outer", Path: []*osm.Node{mockNode(31, 44.1, 33.6), mockNode(32, 44.6, 34.8)}},
			{Role: "outer", Path: []*osm.Node{mockNode(41, 44.7, 35.3), mockNode(42, 44.8, 35.6)}},
		},
		waySlice{
			{Role: "outer", Path: []*osm.Node{
				mockNode(31, 44.1, 33.6),
				mockNode(32, 44.6, 34.8),
				mockNode(41, 44.7, 35.3),
				mockNode(42, 44.8, 35.6),
				mockNode(14, 44.9, 36.6),
				mockNode(13, 46.1, 42.8),
				mockNode(12, 48.9, 37.3),
				mockNode(11, 45.6, 36.6),
				mockNode(21, 45.7, 35.2),
				mockNode(22, 45.8, 32.9),
				mockNode(31, 44.1, 33.6),
			}},
		},
	),
)

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/osmx")
}

// --------------------------------------------------------------------

func mockNode(nodeID int64, lat, lng float64) *osm.Node {
	return &osm.Node{Elem: osm.Elem{ID: nodeID}, Lat: lat, Lng: lng}
}

func mockNodes(nodeIDs ...int64) []*osm.Node {
	nodes := make([]*osm.Node, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		nodes = append(nodes, &osm.Node{Elem: osm.Elem{ID: n}})
	}

	return nodes
}
