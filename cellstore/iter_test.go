package cellstore

import (
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Iterator", func() {
	var subject *Iterator
	var reader *Reader

	BeforeEach(func() {
		reader = seedReader(100)

		var err error
		subject, err = reader.FindBlock(1317624576600000297)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		subject.Release()
	})

	It("should have info", func() {
		Expect(subject.index).To(HaveLen(4))
		Expect(subject.bnum).To(Equal(2))
		Expect(subject.snum).To(Equal(0))

		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))
	})

	It("should iterate blocks", func() {
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))
		Expect(string(subject.Value())).To(ContainSubstring(subject.CellID().String()))

		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000249)))
		Expect(string(subject.Value())).To(ContainSubstring(subject.CellID().String()))
	})

	It("should iterate blocks", func() {
		Expect(subject.NextBlock()).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000361)))

		Expect(subject.PrevBlock()).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.PrevBlock()).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000121)))

		Expect(subject.PrevBlock()).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000001)))

		Expect(subject.PrevBlock()).To(BeFalse())
	})

	It("should prevent block moves when the beginning/end is reached", func() {
		it1, err := reader.FindBlock(1317624570000000001)
		Expect(err).NotTo(HaveOccurred())
		defer it1.Release()

		Expect(it1.Next()).To(BeTrue())
		Expect(it1.CellID()).To(Equal(s2.CellID(1317624576600000001)))
		Expect(it1.PrevBlock()).To(BeFalse())

		it2, err := reader.FindBlock(1317624576600000751)
		Expect(err).NotTo(HaveOccurred())
		defer it2.Release()

		Expect(it2.Next()).To(BeTrue())
		Expect(it2.CellID()).To(Equal(s2.CellID(1317624576600000721)))
		Expect(it2.NextBlock()).To(BeFalse())
	})

	It("should go to sections", func() {
		Expect(subject.toSection(1)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.toSection(0)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.toSection(2)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000305)))

		Expect(subject.toSection(3)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		Expect(subject.toSection(4)).To(BeFalse())
	})

	It("should seek sections", func() {
		subject.SeekSection(1317624576600000240)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.SeekSection(1317624576600000241)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.SeekSection(1317624576600000251)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.SeekSection(1317624576600000265)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.SeekSection(1317624576600000267)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.SeekSection(1317624576600000273)
		Expect(subject.snum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		subject.SeekSection(1317624576600000297)
		Expect(subject.snum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		subject.SeekSection(1317624576600000345)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		subject.SeekSection(1317624576600000353)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		subject.SeekSection(1317624576600000357)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		subject.SeekSection(1317624576600000317)
		Expect(subject.snum).To(Equal(2))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000305)))
	})

	It("should seek entries", func() {
		subject.Seek(1317624576600000240)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.Seek(1317624576600000241)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		subject.Seek(1317624576600000251)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000257)))

		subject.Seek(1317624576600000265)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000265)))

		subject.Seek(1317624576600000267)
		Expect(subject.snum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		subject.Seek(1317624576600000273)
		Expect(subject.snum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		subject.Seek(1317624576600000297)
		Expect(subject.snum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000297)))

		subject.Seek(1317624576600000345)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000345)))

		subject.Seek(1317624576600000353)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000353)))

		subject.Seek(1317624576600000357)
		Expect(subject.snum).To(Equal(3))
		Expect(subject.Next()).To(BeFalse())

		subject.Seek(1317624576600000317)
		Expect(subject.snum).To(Equal(2))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000321)))
	})

	It("should forward", func() {
		it, err := reader.FindBlock(1317624576600000701)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()
		it.Seek(1317624576600000701)

		var (
			cells []s2.CellID
			bnums []int
			boffs []int
		)

		it.fwd(func(cellID s2.CellID, bnum, boff int) bool {
			cells = append(cells, cellID)
			bnums = append(bnums, bnum)
			boffs = append(boffs, boff)
			return true
		})
		Expect(cells).To(coverRange(1317624576600000705, 1317624576600000793))
		Expect(bnums).To(Equal([]int{5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}))
		Expect(boffs).To(Equal([]int{1735, 1866, 0, 139, 270, 401, 532, 671, 802, 933, 1064, 1203}))
	})

	It("should forward until condition", func() {
		it, err := reader.FindBlock(1317624576600000701)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()
		it.Seek(1317624576600000701)

		var cells []s2.CellID
		it.fwd(func(cellID s2.CellID, bnum, boff int) bool {
			cells = append(cells, cellID)
			return len(cells) < 5
		})
		Expect(cells).To(coverRange(1317624576600000705, 1317624576600000737))
	})

	It("should reverse", func() {
		it, err := reader.FindBlock(1317624576600000171)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()
		it.Seek(1317624576600000171)

		var (
			cells []s2.CellID
			bnums []int
			boffs []int
		)

		it.rev(func(cellID s2.CellID, bnum, boff int) bool {
			cells = append(cells, cellID)
			bnums = append(bnums, bnum)
			boffs = append(boffs, boff)
			return true
		})
		Expect(cells).To(Equal([]s2.CellID{
			1317624576600000153, 1317624576600000161, 1317624576600000169, // block 1, section 1
			1317624576600000121, 1317624576600000129, 1317624576600000137, 1317624576600000145, // block 1, section 0
			1317624576600000097, 1317624576600000105, 1317624576600000113, // block 0, section 3
			1317624576600000065, 1317624576600000073, 1317624576600000081, 1317624576600000089, // block 0, section 2
			1317624576600000033, 1317624576600000041, 1317624576600000049, 1317624576600000057, // block 0, section 1
			1317624576600000001, 1317624576600000009, 1317624576600000017, 1317624576600000025, // block 0, section 0
		}))
		Expect(bnums).To(Equal([]int{
			1, 1, 1,
			1, 1, 1, 1,
			0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0,
		}))
		Expect(boffs).To(Equal([]int{
			532, 671, 802,
			0, 139, 270, 401,
			1596, 1735, 1866,
			1064, 1203, 1334, 1465,
			532, 671, 802, 933,
			0, 139, 270, 401,
		}))
	})

	It("should reverse until condition", func() {
		it, err := reader.FindBlock(1317624576600000171)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()
		it.Seek(1317624576600000171)

		var cells []s2.CellID
		it.rev(func(cellID s2.CellID, bnum, boff int) bool {
			cells = append(cells, cellID)
			return len(cells) < 11
		})
		Expect(cells).To(coverRange(1317624576600000153, 1317624576600000065))
	})

})
