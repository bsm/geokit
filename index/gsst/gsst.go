// Package gsst implements an index store on opt of golang/leveldb/table
package gsst

import (
	"os"

	"github.com/bsm/geokit/index"
	"github.com/golang/leveldb/db"
	"github.com/golang/leveldb/table"
)

type reader struct{ *table.Reader }

// Open opens a new Reader
func Open(fname string) (index.StoreReader, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	return &reader{
		Reader: table.NewReader(f, nil),
	}, nil
}

func (r *reader) Get(key []byte) ([]byte, error) {
	val, err := r.Reader.Get(key, nil)
	if err == db.ErrNotFound {
		return nil, nil
	}
	return val, err
}

// --------------------------------------------------------------------

type writer struct{ *table.Writer }

// Create creates a new Writer
func Create(fname string) (index.StoreWriter, error) {
	f, err := os.Create(fname)
	if err != nil {
		return nil, err
	}

	return &writer{
		Writer: table.NewWriter(f, nil),
	}, nil
}

func (w *writer) Put(key, value []byte) error {
	return w.Writer.Set(key, value, nil)
}
