package lsst

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/bsm/geokit/index"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader/Writer", func() {
	var subject index.StoreReader
	var dir string

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "geo-index-lsst-test")
		Expect(err).NotTo(HaveOccurred())

		w, err := CreateFile(dir+"/data.sst", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Put([]byte("key"), []byte("testdata"))).To(Succeed())
		Expect(w.Close()).To(Succeed())

		subject, err = OpenFile(dir+"/data.sst", nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(os.RemoveAll(dir)).To(Succeed())
	})

	It("should succeed in querying for the correct value", func() {
		Expect(subject.Get([]byte("key"))).To(Equal([]byte("testdata")))
	})

	It("should return nil as value does not exist", func() {
		Expect(subject.Get([]byte("notfound"))).To(BeNil())
	})

})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/index/lsst")
}
