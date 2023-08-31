package cellstore_test

import (
	"github.com/bsm/geokit/cellstore"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("NearbyRS", func() {
	It("should sort",
		func() {
			rs := &cellstore.NearbyRS{
				Entries: []cellstore.NearbyEntry{
					{CellID: 1317624576600000345, Distance: 60},
					{CellID: 1317624576600000321, Distance: 30},
					{CellID: 1317624576600000305, Distance: 10},
					{CellID: 1317624576600000289, Distance: 20},
					{CellID: 1317624576600000257, Distance: 40},
					{CellID: 1317624576600000249, Distance: 70},
					{CellID: 1317624576600000241, Distance: 50},
				},
			}
			rs.Sort()

			Expect(rs.Entries).To(Equal([]cellstore.NearbyEntry{
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
