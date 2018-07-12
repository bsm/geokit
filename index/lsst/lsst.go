// Package lsst implements an index store on opt of syndtr/goleveldb/leveldb/table
package lsst

import (
	"io"
	"os"

	"github.com/bsm/geokit/index"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
)

type reader struct {
	*table.Reader
	closable io.Closer
}

// OpenFile opens a new Reader
func OpenFile(fname string, o *opt.Options) (index.StoreReader, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	rd, err := Open(f, fi.Size(), o)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	rd.(*reader).closable = f
	return rd, nil
}

// Open opens a new Reader
func Open(ra io.ReaderAt, sz int64, o *opt.Options) (index.StoreReader, error) {
	fd := storage.FileDesc{Type: storage.TypeTable}
	tr, err := table.NewReader(ra, sz, fd, nil, nil, o)
	if err != nil {
		return nil, err
	}
	return &reader{Reader: tr}, nil
}

func (r *reader) Get(key []byte) ([]byte, error) {
	val, err := r.Reader.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return val, err
}

func (r *reader) Close() error {
	if r.closable != nil {
		return r.closable.Close()
	}
	return nil
}

// --------------------------------------------------------------------

type writer struct{ *table.Writer }

// CreateFile creates a new Writer
func CreateFile(fname string, o *opt.Options) (index.StoreWriter, error) {
	f, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	return Create(f, o)
}

// Create creates a new Writer
func Create(w io.Writer, o *opt.Options) (index.StoreWriter, error) {
	return &writer{
		Writer: table.NewWriter(w, o),
	}, nil
}

func (w *writer) Put(key, value []byte) error {
	return w.Writer.Append(key, value)
}
