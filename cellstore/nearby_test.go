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
	const numSeeds = 100

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
		reader = seedReader(numSeeds)
	})

	It("should iterate", func() {
		it, err := reader.Nearby(1317624576600000281, 20)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000281)))
		Expect(it.Distance()).To(BeNumerically("~", 0.0, 0.0))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000257)))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000329)))
		Expect(it.Distance()).To(BeNumerically("~", 5.7e-9, 1e-10))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))

		for i := 0; i < 6; i++ {
			Expect(it.Next()).To(BeTrue())
		}
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000241)))
		Expect(it.Distance()).To(BeNumerically("~", 6.4e-9, 1e-10))
		Expect(string(it.Value())).To(ContainSubstring(it.CellID().String()))
	})

	It("should iterate when origin less than lower bound", func() {
		it, err := reader.Nearby(seedCellID-100, 1)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000009)))
		Expect(it.Distance()).To(BeNumerically("~", 1.2e-8, 1e-9))
		Expect(it.Next()).To(BeFalse())
	})

	It("should iterate when origin greater than upper bound", func() {
		it, err := reader.Nearby(seedCellID+8*numSeeds+100, 1)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000777)))
		Expect(it.Distance()).To(BeNumerically("~", 6.1e-9, 1e-10))
		Expect(it.Next()).To(BeFalse())
	})

	It("should sort and limit", func() {
		Expect(nearby(3)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
		}))
		Expect(nearby(4)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225, // new addition
		}))
		Expect(nearby(5)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225,
			1317624576600000313, // new addition
		}))
		Expect(nearby(7)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225,
			1317624576600000313,
			1317624576600000305, // new addition
			1317624576600000257, // new addition
		}))
		Expect(nearby(9)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225,
			1317624576600000313,
			1317624576600000305,
			1317624576600000257,
			1317624576600000217, // new addition
			1317624576600000265, // new addition
		}))
		Expect(nearby(13)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225,
			1317624576600000313,
			1317624576600000305,
			1317624576600000257,
			1317624576600000217,
			1317624576600000265,
			1317624576600000297, // new addition
			1317624576600000321, // new addition
			1317624576600000329, // new addition
			1317624576600000249, // new addition
		}))
		Expect(nearby(40)).To(Equal([]s2.CellID{
			1317624576600000281,
			1317624576600000289,
			1317624576600000273,
			1317624576600000225,
			1317624576600000313,
			1317624576600000305,
			1317624576600000257,
			1317624576600000217,
			1317624576600000265,
			1317624576600000297,
			1317624576600000321,
			1317624576600000425,
			1317624576600000329,
			1317624576600000249,
			1317624576600000361, // new addition
			1317624576600000233, // new addition
			1317624576600000417, // new addition
			1317624576600000209, // new addition
			1317624576600000241, // new addition
			1317624576600000369, // new addition
			1317624576600000081, // new addition
			1317624576600000433, // new addition
			1317624576600000089, // new addition
			1317624576600000193, // new addition
			1317624576600000409, // new addition
			1317624576600000337, // new addition
			1317624576600000377, // new addition
			1317624576600000201, // new addition
			1317624576600000441, // new addition
			1317624576600000353, // new addition
			1317624576600000345, // new addition
			1317624576600000185, // new addition
			1317624576600000073, // new addition
			1317624576600000401, // new addition
			1317624576600000097, // new addition
			1317624576600000457, // new addition
			1317624576600000145, // new addition
			1317624576600000385, // new addition
			1317624576600000449, // new addition
			1317624576600000137, // new addition
		}))
	})

})

var _ = Describe("nearbySlice", func() {
	It("should sort",
		func() {
			s := nearbySlice{
				{CellID: 1317624576600000345, distance: 60},
				{CellID: 1317624576600000321, distance: 30},
				{CellID: 1317624576600000305, distance: 10},
				{CellID: 1317624576600000289, distance: 20},
				{CellID: 1317624576600000257, distance: 40},
				{CellID: 1317624576600000249, distance: 70},
				{CellID: 1317624576600000241, distance: 50},
			}
			s.SortByDistance()
			Expect(s).To(Equal(nearbySlice{
				{CellID: 1317624576600000305, distance: 10},
				{CellID: 1317624576600000289, distance: 20},
				{CellID: 1317624576600000321, distance: 30},
				{CellID: 1317624576600000257, distance: 40},
				{CellID: 1317624576600000241, distance: 50},
				{CellID: 1317624576600000345, distance: 60},
				{CellID: 1317624576600000249, distance: 70},
			}))
		})
})

// --------------------------------------------------------------------

func BenchmarkReader_Nearby(b *testing.B) {
	runBench := func(b *testing.B, numRecords int, limit int) {
		fname, err := seedTempFile(numRecords, NoCompression)
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(fname)

		r, closer, err := openSeed(fname, false)
		if err != nil {
			b.Fatal(err)
		}
		defer closer.Close()

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

	b.Run("limit:1", func(b *testing.B) {
		runBench(b, 1e7, 1)
	})
	b.Run("limit:5", func(b *testing.B) {
		runBench(b, 1e7, 5)
	})
	b.Run("limit:20", func(b *testing.B) {
		runBench(b, 1e7, 20)
	})
	b.Run("limit:100", func(b *testing.B) {
		runBench(b, 1e7, 100)
	})
}
