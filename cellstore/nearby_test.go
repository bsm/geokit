package cellstore

import (
	"os"
	"testing"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NearbyIterator", func() {
	var reader *Reader

	nearby := func(limit int) ([]s2.CellID, error) {
		it, err := reader.Nearby(1317624576600000281, limit)
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

	It("should iterate", func() {
		it, err := reader.Nearby(1317624576600000281, 20)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000209)))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000257)))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000305)))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000417)))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))
	})

	It("should sort and limit", func() {
		Expect(nearby(3)).To(Equal([]s2.CellID{
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
		}))
		Expect(nearby(4)).To(Equal([]s2.CellID{
			1317624576600000225, // new addition
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
		}))
		Expect(nearby(5)).To(Equal([]s2.CellID{
			1317624576600000225,
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
			1317624576600000313, // new addition
		}))
		Expect(nearby(6)).To(Equal([]s2.CellID{
			1317624576600000225,
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
			1317624576600000305, // new addition
			1317624576600000313,
		}))
		Expect(nearby(7)).To(Equal([]s2.CellID{
			1317624576600000225,
			1317624576600000257, // new addition
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
			1317624576600000305,
			1317624576600000313,
		}))
		Expect(nearby(10)).To(Equal([]s2.CellID{
			1317624576600000217, // new addition
			1317624576600000225,
			1317624576600000257,
			1317624576600000265, // new addition
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
			1317624576600000297, // new addition
			1317624576600000305,
			1317624576600000313,
		}))
		Expect(nearby(40)).To(Equal([]s2.CellID{
			1317624576600000073,
			1317624576600000081,
			1317624576600000089,
			1317624576600000097,
			1317624576600000137,
			1317624576600000145,
			1317624576600000185,
			1317624576600000193,
			1317624576600000201,
			1317624576600000209,
			1317624576600000217,
			1317624576600000225,
			1317624576600000233,
			1317624576600000241,
			1317624576600000249,
			1317624576600000257,
			1317624576600000265,
			1317624576600000273,
			1317624576600000281,
			1317624576600000289,
			1317624576600000297,
			1317624576600000305,
			1317624576600000313,
			1317624576600000321,
			1317624576600000329,
			1317624576600000337,
			1317624576600000345,
			1317624576600000353,
			1317624576600000361,
			1317624576600000369,
			1317624576600000377,
			1317624576600000385,
			1317624576600000401,
			1317624576600000409,
			1317624576600000417,
			1317624576600000425,
			1317624576600000433,
			1317624576600000441,
			1317624576600000449,
			1317624576600000457,
		}))
	})

})

var _ = Describe("nearbySlice", func() {

	It("should sort",
		func() {
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

})

// --------------------------------------------------------------------

func BenchmarkReader_Nearby(b *testing.B) {
	runBench := func(b *testing.B, numRecords int, limit int) {
		r, f, err := seedReaderOnDisk(numRecords, NoCompression)
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(f.Name())
		defer f.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cellID := seedCellID + s2.CellID((i%numRecords)*8)

			it, err := r.Nearby(cellID, limit)
			if err != nil {
				b.Fatalf("error finding nearby %d: %v", cellID, err)
			}

			n := 0
			for it.Next() {
				n++
			}
			if err := it.Err(); err != nil {
				b.Fatalf("error iterating over block containing cell %d: %v", cellID, err)
			}
			it.Release()

			if n != limit {
				b.Fatalf("unable to iterate across %d, expected %d entries but got %d", cellID, limit, n)
			}
		}
	}

	b.Run("1k limit=3", func(b *testing.B) {
		runBench(b, 1000, 3)
	})
	b.Run("1M limit=3", func(b *testing.B) {
		runBench(b, 1*1000*1000, 3)
	})
	b.Run("1k limit=100", func(b *testing.B) {
		runBench(b, 1000, 100)
	})
	b.Run("1M limit=100", func(b *testing.B) {
		runBench(b, 1*1000*1000, 100)
	})
}
