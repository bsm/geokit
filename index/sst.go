package index

import (
	"encoding/binary"
	"os"

	"github.com/golang/geo/s2"
	"github.com/golang/leveldb/db"
	"github.com/golang/leveldb/table"
)

// SSTReader represents a reader for SST tables
type SSTReader struct{ r *table.Reader }

// OpenSST opens a new SST reader
func OpenSST(fname string) (*SSTReader, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	return &SSTReader{
		r: table.NewReader(f, nil),
	}, nil
}

// Get retrieves data stored by cellID
func (r *SSTReader) Get(cellID s2.CellID) ([]byte, error) {
	val, err := r.r.Get(sstKey(cellID), nil)
	if err == db.ErrNotFound {
		return nil, nil
	}
	return val, err
}

// Close closes the reader
func (r *SSTReader) Close() error { return r.r.Close() }

// --------------------------------------------------------------------

// SSTWriter instances build new SST indices
type SSTWriter struct {
	w *table.Writer
}

// CreateSST starts a new writer
func CreateSST(fname string) (*SSTWriter, error) {
	f, err := os.Create(fname)
	if err != nil {
		return nil, err
	}

	return &SSTWriter{
		w: table.NewWriter(f, nil),
	}, nil
}

// Put appends a new record
func (w *SSTWriter) Put(cellID s2.CellID, val []byte) error {
	return w.w.Set(sstKey(cellID), val, nil)
}

// Close closes and flushes the writer
func (w *SSTWriter) Close() error { return w.w.Close() }

// --------------------------------------------------------------------

func sstKey(cellID s2.CellID) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(cellID))
	return key
}
