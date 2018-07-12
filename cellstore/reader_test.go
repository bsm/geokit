package cellstore

import (
	"bytes"
	"fmt"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var buf *bytes.Buffer
	var subject *Reader

	var cellID = s2.CellID(1317624576600000001)
	var value = bytes.Repeat([]byte{'x'}, 256)

	var setup = func(n int) {
		buf = new(bytes.Buffer)

		w := NewWriter(buf, &Options{BlockSize: 2 * KiB})
		for i := 0; i < 8*n; i += 8 {
			Expect(w.Append(cellID+s2.CellID(i), value)).To(Succeed())
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
			if n := len(it.Value()); n != 256 {
				return nil, fmt.Errorf("expected values to be 256 bytes, but was %d", n)
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
		setup(1000)
	})

	It("should init", func() {
		Expect(subject.NumBlocks()).To(Equal(143))

		setup(50000)
		Expect(subject.NumBlocks()).To(Equal(7143))
	})

	It("should find blocks", func() {
		Expect(minMax(findBlock(cellID - 2))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000049}))
		Expect(minMax(findBlock(1317624576600000001))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000049}))
		Expect(minMax(findBlock(1317624576600000049))).To(Equal([]s2.CellID{1317624576600000001, 1317624576600000049}))
		Expect(minMax(findBlock(1317624576600000055))).To(Equal([]s2.CellID{1317624576600000057, 1317624576600000105}))
		Expect(minMax(findBlock(1317624576600000057))).To(Equal([]s2.CellID{1317624576600000057, 1317624576600000105}))
		Expect(minMax(findBlock(1317624576600000105))).To(Equal([]s2.CellID{1317624576600000057, 1317624576600000105}))
		Expect(minMax(findBlock(1317624576600007953))).To(Equal([]s2.CellID{1317624576600007953, 1317624576600007993}))
		Expect(minMax(findBlock(1317624576600007993))).To(Equal([]s2.CellID{1317624576600007953, 1317624576600007993}))
		Expect(minMax(findBlock(1317624576600008001))).To(Equal([]s2.CellID{1317624576600007953, 1317624576600007993}))

		Expect(findBlock(1317624576600004553)).To(Equal([]s2.CellID{
			1317624576600004537,
			1317624576600004545,
			1317624576600004553,
			1317624576600004561,
			1317624576600004569,
			1317624576600004577,
			1317624576600004585,
		}))
		Expect(findBlock(1317624576600004557)).To(Equal([]s2.CellID{
			1317624576600004537,
			1317624576600004545,
			1317624576600004553,
			1317624576600004561,
			1317624576600004569,
			1317624576600004577,
			1317624576600004585,
		}))
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
		it, err := subject.FindBlock(1317624576600004553)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600004537)))

		Expect(it.NextBlock()).To(Succeed())
		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600004593)))

		Expect(it.PrevBlock()).To(Succeed())
		Expect(it.Next()).To(BeTrue())
		Expect(it.CellID()).To(Equal(s2.CellID(1317624576600004537)))
	})

	It("should prevent block moves when the beginning/end is reached", func() {
		it1, err := subject.FindBlock(1317624570000000001)
		Expect(err).NotTo(HaveOccurred())
		defer it1.Release()

		Expect(it1.Next()).To(BeTrue())
		Expect(it1.CellID()).To(Equal(s2.CellID(1317624576600000001)))
		Expect(it1.PrevBlock()).To(MatchError(ErrBlockUnavailable))

		it2, err := subject.FindBlock(1317624576600008001)
		Expect(err).NotTo(HaveOccurred())
		defer it2.Release()

		Expect(it2.Next()).To(BeTrue())
		Expect(it2.CellID()).To(Equal(s2.CellID(1317624576600007953)))
		Expect(it2.NextBlock()).To(MatchError(ErrBlockUnavailable))
	})

})
