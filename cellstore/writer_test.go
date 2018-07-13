package cellstore

import (
	"bytes"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var buf *bytes.Buffer
	var subject *Writer

	var cellID = s2.CellID(1317624576600000001)
	var value = bytes.Repeat([]byte{'x'}, 256)

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		subject = NewWriter(buf, nil)
	})

	AfterEach(func() {
		_ = subject.Close()
	})

	It("should write empty", func() {
		Expect(subject.Close()).To(Succeed())
		Expect(buf.Len()).To(Equal(26))
	})

	It("should prevent out-of-order writes", func() {
		Expect(subject.Append(cellID, value)).To(Succeed())
		Expect(subject.Append(cellID, value)).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022030000 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(cellID-2, value)).To(MatchError(`cellstore: attempted an out-of-order append, 0/210210210210210201302022023333 must be > 0/210210210210210201302022030000`))
		Expect(subject.Append(cellID+2, value)).To(Succeed())
	})

	It("should prevent invalid writes", func() {
		Expect(subject.Append(cellID-1, value)).To(MatchError(errInvalidCellID))
		Expect(subject.Append(cellID+1, value)).To(MatchError(errInvalidCellID))
	})

	It("should write", func() {
		for i := 0; i < 100000; i += 2 {
			Expect(subject.Append(cellID+s2.CellID(i), value)).To(Succeed())
		}
		Expect(subject.Close()).To(Succeed())
		Expect(len(subject.index)).To(Equal(1667))
		Expect(buf.Len()).To(BeNumerically("~", 838709, 1000))
		Expect(buf.Bytes()[buf.Len()-8:]).To(Equal(magic))
	})

})
