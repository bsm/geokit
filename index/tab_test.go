package index

import (
	"io/ioutil"
	"os"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tab", func() {
	var dir string
	var c1 = s2.CellIDFromToken("00000001")
	var c2 = s2.CellIDFromToken("00000002")
	var c3 = s2.CellIDFromToken("00000003")

	BeforeEach(func() {
		var err error
		dir, err = ioutil.TempDir("", "geo-index-tab-test")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dir)).To(Succeed())
	})

	var runTest = func(fname string, expSize int64) {
		w, err := AppendTab(fname)
		Expect(err).NotTo(HaveOccurred())
		Expect(w.Put(c1, []byte("DATA1"))).To(Succeed())
		Expect(w.Put(c1, []byte("DATA2"))).To(Succeed())
		Expect(w.Put(c2, []byte("DATA3"))).To(Succeed())
		Expect(w.Put(c3, []byte("DATA4"))).To(Succeed())
		Expect(w.Put(c3, []byte("DATA5"))).To(Succeed())
		Expect(w.Close()).To(Succeed())

		stat, err := os.Stat(fname)
		Expect(err).NotTo(HaveOccurred())
		Expect(stat.Size()).To(BeNumerically("~", expSize, 10))

		r, err := OpenTab(fname)
		Expect(err).NotTo(HaveOccurred())

		cellID, vals, err := r.Read()
		Expect(err).NotTo(HaveOccurred())
		Expect(cellID).To(Equal(c1))
		Expect(vals).To(Equal([][]byte{
			[]byte("DATA1"),
			[]byte("DATA2"),
		}))

		cellID, vals, err = r.Read()
		Expect(err).NotTo(HaveOccurred())
		Expect(cellID).To(Equal(c2))
		Expect(vals).To(Equal([][]byte{
			[]byte("DATA3"),
		}))

		cellID, vals, err = r.Read()
		Expect(err).NotTo(HaveOccurred())
		Expect(cellID).To(Equal(c3))
		Expect(vals).To(Equal([][]byte{
			[]byte("DATA4"),
			[]byte("DATA5"),
		}))

		_, _, err = r.Read()
		Expect(err).To(MatchError("EOF"))
		Expect(r.Close()).To(Succeed())
	}

	It("should write/read plain data", func() {
		runTest(dir+"/data.tab", 102)
	})

	It("should write/read compressed data", func() {
		runTest(dir+"/data.tab.gz", 83)
	})

})
