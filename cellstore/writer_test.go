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
	var cellID = s2.CellID(1317624576600000001)

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
		Expect(subject.Append(cellID, []byte("testdata"))).To(Succeed())
		Expect(subject.Append(cellID, []byte("testdata"))).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022030000 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(cellID-2, []byte("testdata"))).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022023333 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(cellID+2, []byte("testdata"))).To(Succeed())
	})

	It("should prevent invalid writes", func() {
		Expect(subject.Append(cellID-1, []byte("testdata"))).To(MatchError(errInvalidCellID))
		Expect(subject.Append(cellID+1, []byte("testdata"))).To(MatchError(errInvalidCellID))
	})

	It("should write (non-compressable)", func() {
		rnd := rand.New(rand.NewSource(1))
		val := make([]byte, 128)

		for i := 0; i < 100000; i += 2 {
			_, err := rnd.Read(val)
			Expect(err).NotTo(HaveOccurred())
			Expect(subject.Append(cellID+s2.CellID(i), val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(len(subject.index)).To(Equal(807))
		Expect(buf.Len()).To(BeNumerically("~", 6562935, KiB))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})

	It("should write (well-compressable)", func() {
		val := bytes.Repeat([]byte("testdata"), 16)
		for i := 0; i < 100000; i += 2 {
			Expect(subject.Append(cellID+s2.CellID(i), val)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(len(subject.index)).To(Equal(807))
		Expect(buf.Len()).To(BeNumerically("~", 333914, KiB))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})

})
