package cellstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/exp/mmap"
)

var _ = Describe("Reader", func() {
	var subject *Reader

	findBlock := func(target s2.CellID) ([]s2.CellID, error) {
		it, err := subject.FindBlock(target)
		if err != nil {
			return nil, err
		}
		defer it.Release()

		var res []s2.CellID
		for it.Next() {
			if n := len(it.Value()); n != 128 {
				return nil, fmt.Errorf("expected values to be 128 bytes, but was %d", n)
			}
			res = append(res, it.CellID())
		}
		return res, it.Err()
	}

	BeforeEach(func() {
		subject = seedReader(100)
	})

	It("should init", func() {
		Expect(subject.NumBlocks()).To(Equal(7))
		Expect(subject.index).To(Equal([]blockInfo{
			{MaxCellID: 1317624576600000113, Offset: 0},
			{MaxCellID: 1317624576600000233, Offset: 2014},
			{MaxCellID: 1317624576600000353, Offset: 4028},
			{MaxCellID: 1317624576600000473, Offset: 6042},
			{MaxCellID: 1317624576600000593, Offset: 8056},
			{MaxCellID: 1317624576600000713, Offset: 10070},
			{MaxCellID: 1317624576600000793, Offset: 12084},
		}))

		Expect(seedReader(1000).NumBlocks()).To(Equal(67))
		Expect(seedReader(50000).NumBlocks()).To(Equal(3334))
	})

	It("should find blocks", func() {
		Expect(findBlock(1317624576599999999)).To(coverRange(1317624576600000001, 1317624576600000113))
		Expect(findBlock(1317624576600000001)).To(coverRange(1317624576600000001, 1317624576600000113))
		Expect(findBlock(1317624576600000113)).To(coverRange(1317624576600000001, 1317624576600000113))
		Expect(findBlock(1317624576600000115)).To(coverRange(1317624576600000121, 1317624576600000233))
		Expect(findBlock(1317624576600000121)).To(coverRange(1317624576600000121, 1317624576600000233))
		Expect(findBlock(1317624576600000233)).To(coverRange(1317624576600000121, 1317624576600000233))
		Expect(findBlock(1317624576600000305)).To(coverRange(1317624576600000241, 1317624576600000353))
		Expect(findBlock(1317624576600000397)).To(coverRange(1317624576600000361, 1317624576600000473))
		Expect(findBlock(1317624576600000555)).To(coverRange(1317624576600000481, 1317624576600000593))
		Expect(findBlock(1317624576600000633)).To(coverRange(1317624576600000601, 1317624576600000713))
		Expect(findBlock(1317624576600000721)).To(coverRange(1317624576600000721, 1317624576600000793))
		Expect(findBlock(1317624576600000793)).To(coverRange(1317624576600000721, 1317624576600000793))

		Expect(findBlock(1317624576600000305)).To(Equal([]s2.CellID{
			1317624576600000241, 1317624576600000249, 1317624576600000257, 1317624576600000265,
			1317624576600000273, 1317624576600000281, 1317624576600000289, 1317624576600000297,
			1317624576600000305, 1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findBlock(1317624576600000307)).To(Equal([]s2.CellID{
			1317624576600000241, 1317624576600000249, 1317624576600000257, 1317624576600000265,
			1317624576600000273, 1317624576600000281, 1317624576600000289, 1317624576600000297,
			1317624576600000305, 1317624576600000313, 1317624576600000321, 1317624576600000329,
			1317624576600000337, 1317624576600000345, 1317624576600000353,
		}))
		Expect(findBlock(1317624576600000795)).To(BeEmpty())
	})

	It("should reject invalid cell IDs", func() {
		_, err := subject.FindBlock(1317624576600000002)
		Expect(err).To(MatchError(errInvalidCellID))
	})

	It("should query empty readers", func() {
		it, err := seedReader(0).FindBlock(1317624576600000001)
		Expect(err).NotTo(HaveOccurred())
		defer it.Release()

		Expect(it.Next()).To(BeFalse())
	})
})

// --------------------------------------------------------------------

func seedReader(numRecords int) *Reader {
	buf := new(bytes.Buffer)
	rnd := rand.New(rand.NewSource(1))
	val := make([]byte, 128)

	w := NewWriter(buf, &Options{BlockSize: 2 * KiB, SectionSize: 4})
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

	w := NewWriter(f, &Options{Compression: compression})
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

// --------------------------------------------------------------------

func BenchmarkReader(b *testing.B) {
	runBench := func(b *testing.B, numRecords int, compression Compression, mmaped bool) {
		fname, err := seedTempFile(numRecords, compression)
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(fname)

		r, closer, err := openSeed(fname, mmaped)
		if err != nil {
			b.Fatal(err)
		}
		defer closer.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cellID := seedCellID + s2.CellID((i%numRecords)*8)

			it, err := r.FindBlock(cellID)
			if err != nil {
				b.Fatalf("error finding cell %d: %v", cellID, err)
			}
			if !it.Next() {
				b.Fatalf("unable to advance cursor on %d", cellID)
			}
			if err := it.Err(); err != nil {
				b.Fatalf("error iterating over block containing cell %d: %v", cellID, err)
			}
			it.Release()
		}
	}

	b.Run("10M plain", func(b *testing.B) {
		runBench(b, 1e7, NoCompression, false)
	})
	b.Run("10M mmaped", func(b *testing.B) {
		runBench(b, 1e7, NoCompression, true)
	})
	b.Run("10M snappy", func(b *testing.B) {
		runBench(b, 1e7, SnappyCompression, false)
	})
}
