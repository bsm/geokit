package cellstore_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/bsm/geokit/cellstore"
	"github.com/bsm/sntable"
	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/cellstore")
}

// --------------------------------------------------------------------

const seedCellID = 1317624576600000001

func seedInMem(numRecords int) *cellstore.Reader {
	buf := new(bytes.Buffer)
	rnd := rand.New(rand.NewSource(1))
	val := make([]byte, 128)

	w := cellstore.NewWriter(buf, &sntable.WriterOptions{BlockSize: 2048, BlockRestartInterval: 8})
	for i := 0; i < 8*numRecords; i += 8 {
		_, err := rnd.Read(val)
		Expect(err).NotTo(HaveOccurred())

		cellID := seedCellID + s2.CellID(i)
		copy(val, cellID.String())
		Expect(w.Append(uint64(cellID), val)).To(Succeed())
	}
	Expect(w.Close()).To(Succeed())

	r, err := cellstore.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	Expect(err).NotTo(HaveOccurred())
	return r
}

func createSeeds(numRecords int, compression sntable.Compression) (string, error) {
	f, err := ioutil.TempFile("", "cellstore-bench")
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := sntable.NewWriter(f, &sntable.WriterOptions{Compression: compression})
	defer w.Close()

	v := []byte("testdatatestdatatestdata")
	for i := 0; i < 8*numRecords; i += 8 {
		cellID := seedCellID + s2.CellID(i)
		if err := w.Append(uint64(cellID), v); err != nil {
			_ = f.Close()
			return "", err
		}
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		return "", err
	}
	return f.Name(), f.Close()
}

func openSeeds(name string) (*cellstore.Reader, io.Closer, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	r, err := cellstore.NewReader(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return r, f, nil
}
