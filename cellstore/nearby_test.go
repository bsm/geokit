package cellstore

import (
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NearbyIterator", func() {
	var reader *Reader

	nearby := func(target s2.CellID, limit int) ([]s2.CellID, error) {
		it, err := reader.Nearby(target, limit)
		if err != nil {
			return nil, err
		}
		defer it.Release()

		var res []s2.CellID
		for it.Next() {
			res = append(res, it.CellID())
		}
		return res, it.Err()
	}

	BeforeEach(func() {
		reader = seedReader(100)
	})

	It("should iterate within a block", func() {
		Expect(nearby(1317624576600000281, 3)).To(Equal([]s2.CellID{
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
		}))
		Expect(nearby(1317624576600000281, 4)).To(Equal([]s2.CellID{
			1317624576600000273,
			1317624576600000281,
			1317624576600000289, 1317624576600000305,
		}))
		Expect(nearby(1317624576600000281, 5)).To(Equal([]s2.CellID{
			1317624576600000273,
			1317624576600000281,
			1317624576600000289, 1317624576600000305, 1317624576600000313,
		}))
		Expect(nearby(1317624576600000281, 6)).To(Equal([]s2.CellID{
			1317624576600000257, 1317624576600000273,
			1317624576600000281,
			1317624576600000289, 1317624576600000305, 1317624576600000313,
		}))

		Expect(nearby(1317624576600000321, 7)).To(Equal([]s2.CellID{
			1317624576600000273, 1317624576600000305, 1317624576600000313,
			1317624576600000321,
			1317624576600000329, 1317624576600000337, 1317624576600000345,
		}))

	})

})

var _ = Describe("nearbySlice", func() {

	It("should sort", func() {
		s := nearbySlice{
			{CellID: 1317624576600000345},
			{CellID: 1317624576600000321},
			{CellID: 1317624576600000305},
			{CellID: 1317624576600000289},
			{CellID: 1317624576600000257},
			{CellID: 1317624576600000249},
			{CellID: 1317624576600000241},
		}

		s.SortByDistance(1317624576600000301)
		Expect(s).To(Equal(nearbySlice{
			{CellID: 1317624576600000305},
			{CellID: 1317624576600000289},
			{CellID: 1317624576600000321},
			{CellID: 1317624576600000257},
			{CellID: 1317624576600000241},
			{CellID: 1317624576600000345},
			{CellID: 1317624576600000249},
		}))

		s.Sort()
		Expect(s).To(Equal(nearbySlice{
			{CellID: 1317624576600000241},
			{CellID: 1317624576600000249},
			{CellID: 1317624576600000257},
			{CellID: 1317624576600000289},
			{CellID: 1317624576600000305},
			{CellID: 1317624576600000321},
			{CellID: 1317624576600000345},
		}))
	})

	It("should PushLeft", func() {
		s := make(nearbySlice, 0, 3)
		s = s.PushLeft(nearbyEntry{CellID: 1})
		Expect(s).To(HaveLen(1))
		Expect(s).To(HaveCap(3))

		s = s.PushLeft(nearbyEntry{CellID: 3})
		Expect(s).To(HaveLen(2))
		Expect(s).To(HaveCap(3))

		s = s.PushLeft(nearbyEntry{CellID: 5})
		Expect(s).To(HaveLen(3))
		Expect(s).To(HaveCap(3))
		Expect(s).To(Equal(nearbySlice{
			{CellID: 1},
			{CellID: 3},
			{CellID: 5},
		}))

		s = s.PushLeft(nearbyEntry{CellID: 7})
		Expect(s).To(HaveLen(3))
		Expect(s).To(HaveCap(3))
		Expect(s).To(Equal(nearbySlice{
			{CellID: 3},
			{CellID: 5},
			{CellID: 7},
		}))
	})

	It("should PushRight", func() {
		s := make(nearbySlice, 0, 3)
		s = s.PushRight(nearbyEntry{CellID: 1})
		Expect(s).To(HaveLen(1))
		Expect(s).To(HaveCap(3))

		s = s.PushRight(nearbyEntry{CellID: 3})
		Expect(s).To(HaveLen(2))
		Expect(s).To(HaveCap(3))

		s = s.PushRight(nearbyEntry{CellID: 5})
		Expect(s).To(HaveLen(3))
		Expect(s).To(HaveCap(3))
		Expect(s).To(Equal(nearbySlice{
			{CellID: 1},
			{CellID: 3},
			{CellID: 5},
		}))

		s = s.PushRight(nearbyEntry{CellID: 7})
		Expect(s).To(HaveLen(3))
		Expect(s).To(HaveCap(3))
		Expect(s).To(Equal(nearbySlice{
			{CellID: 1},
			{CellID: 3},
			{CellID: 5},
		}))
	})
})
