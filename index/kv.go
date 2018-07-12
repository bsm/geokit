package index

import (
	"encoding/binary"

	"github.com/golang/geo/s2"
)

// Reader represents a reader on top of key-value stores
type Reader struct{ store StoreReader }

// Open opens a new SST reader
func NewReader(store StoreReader) *Reader { return &Reader{store: store} }

// Get retrieves data stored by cellID
func (r *Reader) Get(cellID s2.CellID) ([]byte, error) {
	return r.store.Get(sstKey(cellID))
}

// --------------------------------------------------------------------

// Writer instances wrap key-value stores
type Writer struct{ store StoreWriter }

// NewWriter opens a new writer
func NewWriter(store StoreWriter) *Writer { return &Writer{store: store} }

// Put appends a new record
func (w *Writer) Put(cellID s2.CellID, value []byte) error {
	return w.store.Put(sstKey(cellID), value)
}

// --------------------------------------------------------------------

func sstKey(cellID s2.CellID) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(cellID))
	return key
}
