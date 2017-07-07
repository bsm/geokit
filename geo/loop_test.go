package geo

import (
	"github.com/golang/geo/s2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Loop", func() {
	var subject *Loop

	BeforeEach(func() {
		subject = &Loop{
			Kind: LoopKindOuter,
			Loop: s2.LoopFromPoints([]s2.Point{
				s2.PointFromLatLng(ne),
				s2.PointFromLatLng(nw),
				s2.PointFromLatLng(sw),
				s2.PointFromLatLng(se),
			}),
		}
	})

	DescribeTable("should show overlaps",
		func(cell s2.Cell, o LoopOverlap) {
			Expect(subject.Overlap(cell)).To(Equal(o))
		},

		// https://codepen.io/sdinh1993/pen/MoXLZB
		Entry("487", s2.CellFromCellID(s2.CellIDFromToken("487")), LoopOverlap_None),
		Entry("874", s2.CellFromCellID(s2.CellIDFromToken("874")), LoopOverlap_ContainedByCell),
		Entry("877", s2.CellFromCellID(s2.CellIDFromToken("877")), LoopOverlap_Partial),
		Entry("87174", s2.CellFromCellID(s2.CellIDFromToken("87174")), LoopOverlap_None),
		Entry("87407", s2.CellFromCellID(s2.CellIDFromToken("87407")), LoopOverlap_ContainsCell),
	)

	It("should marshal/unmarshal", func() {
		bin, err := subject.MarshalBinary()
		Expect(err).NotTo(HaveOccurred())
		Expect(bin).To(HaveLen(102))

		loop := new(Loop)
		Expect(loop.UnmarshalBinary(bin)).To(Succeed())
		Expect(loop.Kind).To(Equal(subject.Kind))
		Expect(loop.Vertices()).To(Equal(subject.Vertices()))
	})

	It("should fill loop", func() {
		cu := subject.Fit(nil, 7)
		cu.Normalize()

		// https://codepen.io/sdinh1993/pen/EXRMxB/
		Expect(cu).To(Equal(s2.CellUnion{
			s2.CellIDFromToken("8708c"),
			s2.CellIDFromToken("87094"),
			s2.CellIDFromToken("870b4"),
			s2.CellIDFromToken("870bc"),
			s2.CellIDFromToken("870d"),
			s2.CellIDFromToken("870f"),
			s2.CellIDFromToken("8711"),
			s2.CellIDFromToken("8713"),
			s2.CellIDFromToken("8715"),
			s2.CellIDFromToken("87164"),
			s2.CellIDFromToken("8716c"),
			s2.CellIDFromToken("8739"),
			s2.CellIDFromToken("873a4"),
			s2.CellIDFromToken("873bc"),
			s2.CellIDFromToken("873c4"),
			s2.CellIDFromToken("873dc"),
			s2.CellIDFromToken("873f"),
			s2.CellIDFromToken("8744"),
			s2.CellIDFromToken("875ac"),
			s2.CellIDFromToken("875b4"),
			s2.CellIDFromToken("876c"),
			s2.CellIDFromToken("87714"),
			s2.CellIDFromToken("8771c"),
			s2.CellIDFromToken("8773"),
			s2.CellIDFromToken("87744"),
			s2.CellIDFromToken("8774c"),
			s2.CellIDFromToken("8776c"),
		}))
	})
})
