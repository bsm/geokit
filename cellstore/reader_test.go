package cellstore

import (
	"bytes"
	"fmt"
	"math/rand"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var buf *bytes.Buffer
	var subject *Reader

	var cellID = s2.CellID(1317624576600000001)
	var setup = func(n int) {
		buf = new(bytes.Buffer)
		rnd := rand.New(rand.NewSource(1))
		val := make([]byte, 128)

		w := NewWriter(buf, &Options{BlockSize: 2 * KiB})
		for i := 0; i < 8*n; i += 8 {
			_, err := rnd.Read(val)
			Expect(err).NotTo(HaveOccurred())

			Expect(w.Append(cellID+s2.CellID(i), val)).To(Succeed())
		}
		Expect(w.Close()).To(Succeed())

		var err error
		subject, err = NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		Expect(err).NotTo(HaveOccurred())
	}

	var findBlock = func(target s2.CellID) ([]s2.CellID, error) {
		it, err := subject.FindBlock(target)
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

	var minMax = func(cells []s2.CellID, err error) ([]s2.CellID, error) {
		if err != nil {
			return nil, err
		}

		var min, max s2.CellID
		for _, c := range cells {
			if v := c; min == 0 || v < min {
				min = v
			}
			if v := c; max == 0 || v > max {
				max = v
			}
		}
		return []s2.CellID{min, max}, nil
	}

	BeforeEach(func() {
		setup(100)
	})

	It("should init", func() {
		Expect(subject.NumBlocks()).To(Equal(7))
		Expect(subject.index).To(Equal([]blockInfo{
			{MaxCellID: 1317624576600000113, Offset: 0},
			{MaxCellID: 1317624576600000233, Offset: 1978},
			{MaxCellID: 1317624576600000353, Offset: 3956},
			{MaxCellID: 1317624576600000473, Offset: 5934},
			{MaxCellID: 1317624576600000593, Offset: 7912},
			{MaxCellID: 1317624576600000713, Offset: 9890},
			{MaxCellID: 1317624576600000793, Offset: 11868},
		}))

		setup(1000)
		Expect(subject.NumBlocks()).To(Equal(67))

		setup(50000)
		Expect(subject.NumBlocks()).To(Equal(3334))
	})

	It("should find blocks", func() {
		Expect(minMax(findBlock(cellID - 2))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000113}))
		Expect(minMax(findBlock(1317624576600000001))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000113}))
		Expect(minMax(findBlock(1317624576600000113))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000113}))
		Expect(minMax(findBlock(1317624576600000115))).To(Equal([]s2.CellID{1317624576600000121, 1317624576600000233}))
		Expect(minMax(findBlock(1317624576600000121))).To(Equal([]s2.CellID{1317624576600000121, 1317624576600000233}))
		Expect(minMax(findBlock(1317624576600000233))).To(Equal([]s2.CellID{1317624576600000121, 1317624576600000233}))
		Expect(minMax(findBlock(1317624576600000721))).To(Equal([]s2.CellID{1317624576600000721, 1317624576600000793}))
		Expect(minMax(findBlock(1317624576600000793))).To(Equal([]s2.CellID{1317624576600000721, 1317624576600000793}))

		Expect(findBlock(1317624576600000305)).To(Equal([]s2.CellID{
			1317624576600000241, 1317624576600000249, 1317624576600000257,
			1317624576600000265, 1317624576600000273, 1317624576600000281,
			1317624576600000289, 1317624576600000297, 1317624576600000305,
			1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findBlock(1317624576600000307)).To(Equal([]s2.CellID{
			1317624576600000241, 1317624576600000249, 1317624576600000257,
			1317624576600000265, 1317624576600000273, 1317624576600000281,
			1317624576600000289, 1317624576600000297, 1317624576600000305,
			1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findBlock(1317624576600000795)).To(BeEmpty())
	})

	It("should reject invalid cell IDs", func() {
		_, err := subject.FindBlock(cellID + 1)
		Expect(err).To(MatchError(errInvalidCellID))
	})

	It("should query empty readers", func() {
		setup(0)

		it, err := subject.FindBlock(cellID)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()
		Expect(it.Next()).To(BeFalse())
	})

	It("should move blocks", func() {
		it, err := subject.FindBlock(1317624576600000305)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(it.NextBlock()).To(Succeed())
		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000361)))

		Expect(it.PrevBlock()).To(Succeed())
		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600000241)))
	})

	It("should prevent block moves when the beginning/end is reached", func() {
		it1, err := subject.FindBlock(1317624570000000001)
		Expect(err).NotTo(HaveOccurred())
		defer it1.Release()

		Expect(it1.Next()).To(BeTrue())
		Expect(it1.CellID()).To(Equal(s2.CellID(1317624576600000001)))
		Expect(it1.PrevBlock()).To(MatchError(ErrBlockUnavailable))

		it2, err := subject.FindBlock(1317624576600000751)
		Expect(err).NotTo(HaveOccurred())
		defer it2.Release()

		Expect(it2.Next()).To(BeTrue())
		Expect(it2.CellID()).To(Equal(s2.CellID(1317624576600000721)))
		Expect(it2.NextBlock()).To(MatchError(ErrBlockUnavailable))
	})

})
