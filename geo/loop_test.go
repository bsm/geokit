package geo

import (
	"github.com/golang/geo/s2"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("should show overlaps",
	func(cell s2.Cell, o LoopOverlap) {
		loop := s2.LoopFromPoints(colorado)
		Expect(LoopIntersectsWithCell(loop, cell)).To(Equal(o))
	},

	// https://codepen.io/bsm/pen/dRqRrp
	Entry("487", s2.CellFromCellID(s2.CellIDFromToken("487")), LoopOverlap_None),
	Entry("874", s2.CellFromCellID(s2.CellIDFromToken("874")), LoopOverlap_ContainedByCell),
	Entry("877", s2.CellFromCellID(s2.CellIDFromToken("877")), LoopOverlap_Partial),
	Entry("87174", s2.CellFromCellID(s2.CellIDFromToken("87174")), LoopOverlap_None),
	Entry("87407", s2.CellFromCellID(s2.CellIDFromToken("87407")), LoopOverlap_ContainsCell),
)

var _ = DescribeTable("should intersect cells",
	func(cell s2.Cell, exp [][]testLL) {
		loop := s2.LoopFromPoints(starshape)
		res := LoopIntersectionWithCell(loop, cell)
		act := make([][]testLL, len(res))
		for i, sub := range res {
			for _, pt := range sub.Vertices() {
				act[i] = append(act[i], testLLFromLL(s2.LatLngFromPoint(pt)))
			}
		}
		Expect(act).To(Equal(exp))
	},

	// https://codepen.io/bsm/full/yXxoBw
	Entry("intersection #1", s2.CellFromCellID(s2.CellIDFromToken("487c")), [][]testLL{
		{
			{Lat: 55.49025118915599, Lng: -0.5485288450023249},
			{Lat: 52.8, Lng: -2.5},
			{Lat: 52.79999999999999, Lng: -2.7999999999999994},
			{Lat: 55.04570014114161, Lng: -3.5241950002328104},
			{Lat: 52.65891336258596, Lng: -3.2286478696356156},
			{Lat: 52.702795575506265, Lng: 0},
			{Lat: 55.4914770123316, Lng: 0},
		},
	}),
	Entry("intersection #2", s2.CellFromCellID(s2.CellIDFromToken("4864")), [][]testLL{
		{
			{Lat: 55.04570014114161, Lng: -3.5241950002328104},
			{Lat: 55.436881634509696, Lng: -3.658825504677576},
			{Lat: 55.341645616168115, Lng: -6.055783348836986},
			{Lat: 54.199999999999996, Lng: -5.9},
			{Lat: 55.29388594664117, Lng: -6.95112363618213},
			{Lat: 55.271147659547914, Lng: -7.338606336236236},
			{Lat: 52.51729571887204, Lng: -6.6302672811716565},
			{Lat: 52.65891336258596, Lng: -3.2286478696356156},
		},
	}),
	Entry("intersection #3", s2.CellFromCellID(s2.CellIDFromToken("485c")), [][]testLL{
		{
			{Lat: 55.098158572302424, Lng: -9.788971248943719},
			{Lat: 52.404341214621056, Lng: -8.402090576646428},
			{Lat: 52.51729571887204, Lng: -6.6302672811716565},
			{Lat: 55.271147659547914, Lng: -7.338606336236236},
		},
		{
			{Lat: 52.40394615622857, Lng: -8.407621812846674},
			{Lat: 53.51622505381126, Lng: -10.653801181254847},
			{Lat: 52.26403022762314, Lng: -10.175510843043208},
		},
	}),
	Entry("intersection #4", s2.CellFromCellID(s2.CellIDFromToken("4884")), [][]testLL{
		{
			{Lat: 56.1807701901571, Lng: 0},
			{Lat: 55.49025118915599, Lng: -0.5485288450023249},
			{Lat: 55.4914770123316, Lng: 0},
		},
	}),
	Entry("intersection #5", s2.CellFromCellID(s2.CellIDFromToken("48f4")), [][]testLL{
		{
			{Lat: 55.83463125668, Lng: -7.495865493920974},
			{Lat: 57.98997729769019, Lng: -9.85664223918145},
			{Lat: 57.857019075856044, Lng: -11.428907681691742},
			{Lat: 55.098158572302424, Lng: -9.788971248943719},
			{Lat: 55.271147659547914, Lng: -7.338606336236236},
		},
	}),
	Entry("intersection #6", s2.CellFromCellID(s2.CellIDFromToken("4894")), [][]testLL{
		{
			{Lat: 58.28440827511659, Lng: -4.728322639913982},
			{Lat: 61.19329709869352, Lng: -6.019464322702787},
			{Lat: 61.14526185564005, Lng: -7.014779887724787},
			{Lat: 58.20698177475088, Lng: -6.489892622888502},
		},
	}),
	Entry("intersection #7", s2.CellFromCellID(s2.CellIDFromToken("48ec")), [][]testLL{
		{
			{Lat: 57.98997729769019, Lng: -9.85664223918145},
			{Lat: 60.66551856508017, Lng: -13.30792398352961},
			{Lat: 60.657175844867204, Lng: -13.39023598510639},
			{Lat: 57.857019075856044, Lng: -11.428907681691742},
		},
	}),
	Entry("intersection #8", s2.CellFromCellID(s2.CellIDFromToken("488c")), [][]testLL{
		{
			{Lat: 55.436881634509696, Lng: -3.658825504677576},
			{Lat: 58.28440827511659, Lng: -4.728322639913982},
			{Lat: 58.20698177475088, Lng: -6.489892622888502},
			{Lat: 55.341645616168115, Lng: -6.055783348836986},
		},
		{
			{Lat: 55.29388594664117, Lng: -6.95112363618213},
			{Lat: 55.83463125668, Lng: -7.495865493920974},
			{Lat: 55.271147659547914, Lng: -7.338606336236236},
		},
	}),
	Entry("intersection #9", s2.CellFromCellID(s2.CellIDFromToken("4844")), [][]testLL{
		{
			{Lat: 52.404341214621056, Lng: -8.402090576646428},
			{Lat: 52.39999999999999, Lng: -8.4},
			{Lat: 52.40394615622857, Lng: -8.407621812846674},
			{Lat: 52.26403022762314, Lng: -10.175510843043208},
			{Lat: 49.648301643763276, Lng: -9.260221531171478},
			{Lat: 49.862303747198254, Lng: -6.027530295521267},
			{Lat: 52.51729571887204, Lng: -6.6302672811716565},
		},
	}),
	Entry("within #1", s2.CellFromCellID(s2.CellIDFromToken("4868")), [][]testLL{
		{
			{Lat: 49.98177401188154, Lng: -2.933398179187317},
			{Lat: 52.65891336258596, Lng: -3.2286478696356156},
			{Lat: 52.51729571887204, Lng: -6.6302672811716565},
			{Lat: 49.862303747198254, Lng: -6.027530295521267},
		},
	}),
	Entry("within #2", s2.CellFromCellID(s2.CellIDFromToken("4874")), [][]testLL{
		{
			{Lat: 50.01876534108111, Lng: 0},
			{Lat: 52.702795575506265, Lng: 0},
			{Lat: 52.65891336258596, Lng: -3.2286478696356156},
			{Lat: 49.98177401188154, Lng: -2.933398179187317},
		},
	}),
	Entry("outside #1", s2.CellFromCellID(s2.CellIDFromToken("489c")), [][]testLL{}),
)

var _ = Describe("FitLoop", func() {
	loop := s2.LoopFromPoints(colorado)

	It("should fit loop", func() {
		cu := FitLoop(loop, nil, 7)
		cu.Normalize()

		// https://codepen.io/bsm/pen/pwOwYq
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

	It("should fit loop-iterate", func() {
		var cellID s2.CellID
		var overlap LoopOverlap
		var n int

		FitLoopDo(loop, 7, func(id s2.CellID, s LoopOverlap) bool {
			n++
			cellID, overlap = id, s
			return false
		})
		Expect(n).To(Equal(1))
		Expect(cellID.ToToken()).To(Equal("8708c"))
		Expect(overlap).To(Equal(LoopOverlap_Partial))
	})

})
