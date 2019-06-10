package cellstore_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/bsm/geokit/cellstore"
	"github.com/bsm/sntable"
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

var _ = Describe("Reader", func() {
	var subject *cellstore.Reader

	findSection := func(target s2.CellID) ([]s2.CellID, error) {
		it, err := subject.FindSection(target)
		if err != nil {
			return nil, err
		}
		defer it.Release()

		var res []s2.CellID
		for it.Next() {
			if n := len(it.Value()); n != 128 {
				return nil, fmt.Errorf("expected values to be 128 bytes, but was %d", n)
			}
			res = append(res, it.CellID())
		}
		return res, it.Err()
	}

	CoverRange := func(min, max s2.CellID) types.GomegaMatcher {
		return WithTransform(func(cells []s2.CellID) []s2.CellID {
			if len(cells) == 0 {
				return nil
			}
			return []s2.CellID{cells[0], cells[len(cells)-1]}
		}, Equal([]s2.CellID{min, max}))
	}

	ContainCells := func(cellIDs ...s2.CellID) types.GomegaMatcher {
		return WithTransform(func(rs *cellstore.NearbyRS) []s2.CellID {
			actual := make([]s2.CellID, 0, rs.Len())
			for _, ent := range rs.Entries {
				actual = append(actual, ent.CellID)
			}
			return actual
		}, Equal(cellIDs))
	}

	BeforeEach(func() {
		subject = seedInMem(100)
	})

	It("should init", func() {
		Expect(subject.NumBlocks()).To(Equal(7))
		Expect(seedInMem(1000).NumBlocks()).To(Equal(67))
		Expect(seedInMem(50000).NumBlocks()).To(Equal(3334))
	})

	It("should find blocks", func() {
		Expect(findSection(1317624576599999999)).To(CoverRange(1317624576600000001, 1317624576600000057))
		Expect(findSection(1317624576600000001)).To(CoverRange(1317624576600000001, 1317624576600000057))
		Expect(findSection(1317624576600000057)).To(CoverRange(1317624576600000001, 1317624576600000057))
		Expect(findSection(1317624576600000059)).To(CoverRange(1317624576600000001, 1317624576600000057))
		Expect(findSection(1317624576600000065)).To(CoverRange(1317624576600000065, 1317624576600000113))
		Expect(findSection(1317624576600000113)).To(CoverRange(1317624576600000065, 1317624576600000113))
		Expect(findSection(1317624576600000305)).To(CoverRange(1317624576600000305, 1317624576600000353))
		Expect(findSection(1317624576600000397)).To(CoverRange(1317624576600000361, 1317624576600000417))
		Expect(findSection(1317624576600000555)).To(CoverRange(1317624576600000545, 1317624576600000593))
		Expect(findSection(1317624576600000633)).To(CoverRange(1317624576600000601, 1317624576600000657))
		Expect(findSection(1317624576600000721)).To(CoverRange(1317624576600000721, 1317624576600000777))
		Expect(findSection(1317624576600000793)).To(CoverRange(1317624576600000785, 1317624576600000793))

		Expect(findSection(1317624576600000305)).To(Equal([]s2.CellID{
			1317624576600000305, 1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findSection(1317624576600000307)).To(Equal([]s2.CellID{
			1317624576600000305, 1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findSection(1317624576600000795)).To(BeEmpty())
	})

	It("should find nearby", func() {
		rs, err := subject.Nearby(1317624576600000281, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rs).To(ContainCells(
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
		))

		ent := rs.Entries[5]
		Expect(ent.CellID).To(Equal(s2.CellID(1317624576600000305)))
		Expect(ent.Distance).To(BeNumerically("~", 4.02e-09, 1e-11))
		Expect(ent.Value).To(HaveLen(128))
		Expect(string(ent.Value[:32])).To(Equal(ent.CellID.String()))

		Expect(subject.Nearby(seedCellID-100, 4)).To(ContainCells(
			1317624576600000009,
			1317624576600000001,
			1317624576600000017,
			1317624576600000025,
		))

		Expect(subject.Nearby(seedCellID+1000, 4)).To(ContainCells(
			1317624576600000777,
			1317624576600000769,
			1317624576600000761,
			1317624576600000785,
		))

		Expect(subject.Nearby(1317624576600000059, 4)).To(ContainCells(
			1317624576600000057,
			1317624576600000049,
			1317624576600000033,
			1317624576600000073,
		))
	})

	It("should reject invalid cell IDs", func() {
		_, err := subject.FindSection(1317624576600000002)
		Expect(err).To(MatchError(`cellstore: invalid cell ID`))
	})

	It("should query empty readers", func() {
		it, err := seedInMem(0).FindSection(1317624576600000001)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeFalse())
	})

	Describe("SectionIterator", func() {
		var iter *cellstore.SectionIterator

		BeforeEach(func() {
			var err error
			iter, err = subject.FindSection(1317624576600000229)
			Expect(err).NotTo(HaveOccurred())

			Expect(iter.BPos()).To(Equal(1))
			Expect(iter.SPos()).To(Equal(1))

			Expect(iter.Next()).To(BeTrue())
			Expect(iter.CellID()).To(Equal(s2.CellID(1317624576600000185)))
		})

		AfterEach(func() {
			Expect(iter.Err()).NotTo(HaveOccurred())

			iter.Release()
			Expect(iter.Err()).To(MatchError(`cellstore: already released`))
		})

		It("should reset", func() {
			for i := 0; i < 4; i++ {
				Expect(iter.NextSection()).To(BeTrue())
			}
			Expect(iter.BPos()).To(Equal(3))
			Expect(iter.SPos()).To(Equal(1))

			Expect(iter.Reset()).To(BeTrue())
			Expect(iter.BPos()).To(Equal(1))
			Expect(iter.SPos()).To(Equal(1))
		})

		It("should move forwards across sections", func() {
			Expect(iter.NextSection()).To(BeTrue())
			Expect(iter.BPos()).To(Equal(2))
			Expect(iter.SPos()).To(Equal(0))
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.CellID()).To(Equal(s2.CellID(1317624576600000241)))

			for i := 0; i < 9; i++ {
				Expect(iter.NextSection()).To(BeTrue())
			}
			Expect(iter.BPos()).To(Equal(6))
			Expect(iter.SPos()).To(Equal(1))
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.CellID()).To(Equal(s2.CellID(1317624576600000785)))

			Expect(iter.NextSection()).To(BeFalse())
		})

		It("should move backwards across sections", func() {
			Expect(iter.PrevSection()).To(BeTrue())
			Expect(iter.BPos()).To(Equal(1))
			Expect(iter.SPos()).To(Equal(0))
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.CellID()).To(Equal(s2.CellID(1317624576600000121)))

			for i := 0; i < 3; i++ {
				Expect(iter.PrevSection()).To(BeTrue())
			}
			Expect(iter.BPos()).To(Equal(0))
			Expect(iter.SPos()).To(Equal(0))
			Expect(iter.Next()).To(BeTrue())
			Expect(iter.CellID()).To(Equal(s2.CellID(1317624576600000001)))

			Expect(iter.PrevSection()).To(BeFalse())
		})
	})
})

// --------------------------------------------------------------------

func BenchmarkReader_Nearby(b *testing.B) {
	runBench := func(b *testing.B, numRecords int, limit int, compression sntable.Compression) {
		fname, err := createSeeds(numRecords, compression)
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(fname)

		r, closer, err := openSeeds(fname)
		if err != nil {
			b.Fatal(err)
		}
		defer closer.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cellID := seedCellID + s2.CellID((i%numRecords)*8)
			rs, err := r.Nearby(cellID, limit)
			if err != nil {
				b.Fatal(err)
			} else if n := rs.Len(); n != limit {
				b.Fatalf("unable to iterate across %d, expected %d entries but got %d", cellID, limit, n)
			}
			rs.Release()
		}
	}

	b.Run("limit:1 plain", func(b *testing.B) {
		runBench(b, 10e6, 1, sntable.NoCompression)
	})
	b.Run("limit:5 plain", func(b *testing.B) {
		runBench(b, 10e6, 5, sntable.NoCompression)
	})
	b.Run("limit:20 plain", func(b *testing.B) {
		runBench(b, 10e6, 20, sntable.NoCompression)
	})
	b.Run("limit:100 plain", func(b *testing.B) {
		runBench(b, 10e6, 100, sntable.NoCompression)
	})

	b.Run("limit:1 snappy", func(b *testing.B) {
		runBench(b, 10e6, 1, sntable.SnappyCompression)
	})
	b.Run("limit:5 snappy", func(b *testing.B) {
		runBench(b, 10e6, 5, sntable.SnappyCompression)
	})
	b.Run("limit:20 snappy", func(b *testing.B) {
		runBench(b, 10e6, 20, sntable.SnappyCompression)
	})
	b.Run("limit:100 snappy", func(b *testing.B) {
		runBench(b, 10e6, 100, sntable.SnappyCompression)
	})
}
