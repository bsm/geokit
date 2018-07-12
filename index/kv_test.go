package index

import (
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader/Writer", func() {
	var subject *Reader
	var store *InMemStore
	var cellID = s2.CellIDFromToken("aaaabbbb")

	BeforeEach(func() {
		store = NewInMemStore()

		w := NewWriter(store)
		Expect(w.Put(cellID, []byte("DATA"))).To(Succeed())

		subject = NewReader(store)
	})

	AfterEach(func() {
		Expect(store.Close()).To(Succeed())
	})

	It("should write", func() {
		Expect(store.Len()).To(Equal(1))
		Expect(store.Get([]byte{0xaa, 0xaa, 0xbb, 0xbb, 0x00, 0x00, 0x00, 0x00})).To(Equal([]byte("DATA")))
	})

	It("should query", func() {
		val, err := subject.Get(cellID)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]byte("DATA")))
	})

	It("should return nil as value does not exist", func() {
		val, err := subject.Get(s2.CellIDFromToken("00008888"))
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(BeNil())
	})

})
