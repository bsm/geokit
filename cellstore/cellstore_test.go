package cellstore

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"golang.org/x/exp/mmap"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/cellstore")
}

const seedCellID = 1317624576600000001

func coverRange(min, max s2.CellID) types.GomegaMatcher {
	return WithTransform(func(cells []s2.CellID) []s2.CellID {
		if len(cells) == 0 {
			return nil
		}
		return []s2.CellID{cells[0], cells[len(cells)-1]}
	}, Equal([]s2.CellID{min, max}))
}

func seedReader(numRecords int) *Reader {
	buf := new(bytes.Buffer)
	rnd := rand.New(rand.NewSource(1))
	val := make([]byte, 128)

	w := NewWriter(buf, &WriterOptions{BlockSize: 2 * KiB, SectionSize: 4})
	for i := 0; i < 8*numRecords; i += 8 {
		_, err := rnd.Read(val)
		Expect(err).NotTo(HaveOccurred())

		cellID := seedCellID + s2.CellID(i)
		copy(val, cellID.String())
		Expect(w.Append(cellID, val)).To(Succeed())
	}
	Expect(w.Close()).To(Succeed())

	r, err := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	Expect(err).NotTo(HaveOccurred())
	return r
}

func seedTempFile(numRecords int, compression Compression) (string, error) {
	f, err := ioutil.TempFile("", "cellstore-bench")
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := NewWriter(f, &WriterOptions{Compression: compression})
	defer w.Close()

	v := []byte("testdatatestdatatestdata")
	for i := 0; i < 8*numRecords; i += 8 {
		cellID := seedCellID + s2.CellID(i)
		if err := w.Append(cellID, v); err != nil {
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

func openSeed(name string, mmaped bool) (*Reader, io.Closer, error) {
	if mmaped {
		return openMmap(name)
	}
	return openFile(name)
}

func openMmap(name string) (*Reader, io.Closer, error) {
	ra, err := mmap.Open(name)
	if err != nil {
		return nil, nil, err
	}

	r, err := NewReader(ra, int64(ra.Len()))
	if err != nil {
		_ = ra.Close()
		return nil, nil, err
	}

	return r, ra, nil
}

func openFile(name string) (*Reader, io.Closer, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}

	r, err := NewReader(f, fi.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return r, f, nil
}
