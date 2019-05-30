package cellstore

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sorter", func() {
	var subject *Sorter

	BeforeEach(func() {
		subject = NewSorter(nil)
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should close", func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should append/sort/iterate", func() {
		Expect(subject.Append(seedCellID, []byte("data1"))).To(Succeed())
		Expect(subject.Append(seedCellID+2, []byte("data2"))).To(Succeed())
		Expect(subject.Append(seedCellID, []byte("data3"))).To(Succeed())
		Expect(subject.Append(seedCellID-2, []byte("data4"))).To(Succeed())
		Expect(subject.Append(seedCellID+4, []byte("data5"))).To(Succeed())
		Expect(subject.Append(seedCellID, []byte("data6"))).To(Succeed())

		iter, err := subject.Sort()
		Expect(err).NotTo(HaveOccurred())

		_, data, err := iter.NextEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([][]byte{[]byte("data4")}))

		_, data, err = iter.NextEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([][]byte{[]byte("data1"), []byte("data3"), []byte("data6")}))

		_, data, err = iter.NextEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([][]byte{[]byte("data2")}))

		_, data, err = iter.NextEntry()
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal([][]byte{[]byte("data5")}))

		_, _, err = iter.NextEntry()
		Expect(err).To(MatchError("EOF"))
	})
})
