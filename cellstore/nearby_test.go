package cellstore_test

import (
	"github.com/bsm/geokit/cellstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NearbySlice", func() {
	It("should sort",
		func() {
			s := cellstore.NearbySlice{
				{CellID: 1317624576600000345, Distance: 60},
				{CellID: 1317624576600000321, Distance: 30},
				{CellID: 1317624576600000305, Distance: 10},
				{CellID: 1317624576600000289, Distance: 20},
				{CellID: 1317624576600000257, Distance: 40},
				{CellID: 1317624576600000249, Distance: 70},
				{CellID: 1317624576600000241, Distance: 50},
			}
			s.Sort()

			Expect(s).To(Equal(cellstore.NearbySlice{
				{CellID: 1317624576600000305, Distance: 10},
				{CellID: 1317624576600000289, Distance: 20},
				{CellID: 1317624576600000321, Distance: 30},
				{CellID: 1317624576600000257, Distance: 40},
				{CellID: 1317624576600000241, Distance: 50},
				{CellID: 1317624576600000345, Distance: 60},
				{CellID: 1317624576600000249, Distance: 70},
			}))
		})
})
