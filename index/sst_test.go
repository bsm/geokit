package index

import (
	"io/ioutil"
	"os"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("SST", func() {
	var subject *SSTReader
	var dir string

	var cellID = s2.CellIDFromToken("aaaabbbb")

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "geo-index-sst-test")
		Expect(err).NotTo(HaveOccurred())

		w, err := CreateSST(dir + "/data.sst")
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Put(cellID, []byte("DATA"))).To(Succeed())
		Expect(w.Close()).To(Succeed())

		subject, err = OpenSST(dir + "/data.sst")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(os.RemoveAll(dir)).To(Succeed())
	})

	It("should succeed in querying for the correct value", func() {
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
