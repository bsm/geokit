package cellstore

import (
	"bytes"
	"math/rand"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var buf *bytes.Buffer
	var subject *Writer

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		subject = NewWriter(buf, nil)
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should write empty", func() {
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(16))
	})

	It("should prevent out-of-order writes", func() {
		Expect(subject.Append(seedCellID, []byte("testdata"))).To(Succeed())
		Expect(subject.Append(seedCellID, []byte("testdata"))).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022030000 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(seedCellID-2, []byte("testdata"))).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022023333 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(seedCellID+2, []byte("testdata"))).To(Succeed())
	})

	It("should prevent invalid writes", func() {
		Expect(subject.Append(seedCellID-1, []byte("testdata"))).To(MatchError(errInvalidCellID))
		Expect(subject.Append(seedCellID+1, []byte("testdata"))).To(MatchError(errInvalidCellID))
	})

	It("should write (non-compressable)", func() {
		rnd := rand.New(rand.NewSource(1))
		val := make([]byte, 128)

		for i := 0; i < 100000; i += 2 {
			_, err := rnd.Read(val)
			Expect(err).NotTo(HaveOccurred())
			Expect(subject.Append(seedCellID+s2.CellID(i), val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(len(subject.index)).To(Equal(404))
		Expect(buf.Len()).To(BeNumerically("~", 6590753, KiB))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})

	It("should write (well-compressable)", func() {
		val := bytes.Repeat([]byte("testdata"), 16)
		for i := 0; i < 100000; i += 2 {
			Expect(subject.Append(seedCellID+s2.CellID(i), val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(len(subject.index)).To(Equal(404))
		Expect(buf.Len()).To(BeNumerically("~", 340474, KiB))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})
})

var _ = Describe("SortWriter", func() {
	var buf *bytes.Buffer
	var subject *SortWriter

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		subject = NewSortWriter(buf, nil)
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should write empty", func() {
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(16))
	})

	It("should allow out-of-order writes", func() {
		Expect(subject.Append(seedCellID, []byte("data1"))).To(Succeed())
		Expect(subject.Append(seedCellID+2, []byte("data2"))).To(Succeed())
		Expect(subject.Append(seedCellID, []byte("data3"))).To(Succeed())
		Expect(subject.Append(seedCellID-2, []byte("data4"))).To(Succeed())
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(60))

		rd, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		Expect(err).NotTo(HaveOccurred())

		iter, err := rd.FindBlock(seedCellID - 100)
		Expect(err).NotTo(HaveOccurred())

		Expect(iter.Next()).To(BeTrue())
		Expect(string(iter.Value())).To(Equal("data4"))
		Expect(iter.Next()).To(BeTrue())
		Expect(string(iter.Value())).To(Equal("data1"))
		Expect(iter.Next()).To(BeTrue())
		Expect(string(iter.Value())).To(Equal("data2"))
		Expect(iter.Next()).To(BeFalse())
		Expect(iter.Err()).NotTo(HaveOccurred())
	})

	It("should prevent invalid writes", func() {
		Expect(subject.Append(seedCellID-1, []byte("testdata"))).To(MatchError(errInvalidCellID))
		Expect(subject.Append(seedCellID+1, []byte("testdata"))).To(MatchError(errInvalidCellID))
	})

	It("should write", func() {
		rnd := rand.New(rand.NewSource(1))
		val := make([]byte, 128)

		for i := 0; i < 100000; i += 2 {
			cellID := seedCellID + s2.CellID((rnd.Int63n(2e6)-1e6)*2)

			_, err := rnd.Read(val)
			Expect(err).NotTo(HaveOccurred())
			Expect(subject.Append(cellID, val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(BeNumerically("~", 6523774, KiB))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})
})
