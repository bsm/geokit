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
		Expect(subject.blockNum).To(Equal(2))
		Expect(subject.sectionNum).To(Equal(0))

		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))
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

	It("should advance sections", func() {
		Expect(subject.advanceSection(1)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.advanceSection(0)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.advanceSection(2)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000305)))

		Expect(subject.advanceSection(3)).To(BeTrue())
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		Expect(subject.advanceSection(4)).To(BeFalse())
	})

	It("should seek sections", func() {
		Expect(subject.SeekSection(1317624576600000240)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.SeekSection(1317624576600000241)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.SeekSection(1317624576600000251)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.SeekSection(1317624576600000265)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.SeekSection(1317624576600000267)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.SeekSection(1317624576600000273)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.SeekSection(1317624576600000297)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(1))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.SeekSection(1317624576600000345)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		Expect(subject.SeekSection(1317624576600000353)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		Expect(subject.SeekSection(1317624576600000357)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(3))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000337)))

		Expect(subject.SeekSection(1317624576600000317)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(2))
		Expect(subject.Next()).To(BeTrue())
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000305)))
	})

	It("should seek entries", func() {
		Expect(subject.Seek(1317624576600000240)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.Seek(1317624576600000241)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000241)))

		Expect(subject.Seek(1317624576600000251)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000257)))

		Expect(subject.Seek(1317624576600000265)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(0))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000265)))

		Expect(subject.Seek(1317624576600000267)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(1))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.Seek(1317624576600000273)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(1))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000273)))

		Expect(subject.Seek(1317624576600000297)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(1))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000297)))

		Expect(subject.Seek(1317624576600000345)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(3))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000345)))

		Expect(subject.Seek(1317624576600000353)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(3))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000353)))

		Expect(subject.Seek(1317624576600000357)).To(BeFalse())

		Expect(subject.Seek(1317624576600000317)).To(BeTrue())
		Expect(subject.sectionNum).To(Equal(2))
		Expect(subject.CellID()).To(Equal(s2.CellID(1317624576600000321)))
	})

})
