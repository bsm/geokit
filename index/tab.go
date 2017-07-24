package index

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/golang/geo/s2"
)

var b64std = base64.StdEncoding

// TabWriter writes a tab-separated key-value file
type TabWriter struct {
	f *os.File
	z *gzip.Writer

	buf []byte
}

func AppendTab(fname string) (*TabWriter, error) {
	if fname == "-" {
		return &TabWriter{}, nil
	}

	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	w := &TabWriter{f: f}
	if strings.HasSuffix(fname, ".gz") {
		w.z = gzip.NewWriter(f)
	}
	return w, nil
}

// Put stores a value
func (w *TabWriter) Put(cellID s2.CellID, val []byte) error {
	var out io.Writer
	if w.z != nil {
		out = w.z
	} else if w.f != nil {
		out = w.f
	} else {
		out = os.Stdout
	}

	n := b64std.EncodedLen(len(val))
	if cap(w.buf) < n {
		w.buf = make([]byte, n)
	} else {
		w.buf = w.buf[:n]
	}
	b64std.Encode(w.buf, val)

	if _, err := fmt.Fprintf(out, "%d\t", cellID); err != nil {
		return err
	}
	if _, err := out.Write(w.buf); err != nil {
		return err
	}
	if _, err := out.Write([]byte{'\n'}); err != nil {
		return err
	}
	return nil
}

// Close closes the writer
func (w *TabWriter) Close() error {
	var err error

	if w.z != nil {
		if e := w.z.Close(); e != nil {
			err = e
		}
	}
	if w.f != nil {
		if e := w.f.Close(); e != nil {
			err = e
		}
	} else {
		if e := os.Stdout.Sync(); e != nil {
			err = e
		}
	}
	return err
}

// --------------------------------------------------------------------

// TabReader reads a tab-separated key-value file
type TabReader struct {
	f  *os.File
	z  *gzip.Reader
	in *bufio.Reader

	stashed struct {
		ID  s2.CellID
		Val []byte
		Err error
	}
}

// OpenTab opens a new TabReader iterator
func OpenTab(fname string) (*TabReader, error) {
	if fname == "-" {
		return &TabReader{in: bufio.NewReader(os.Stdin)}, nil
	}

	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	r := &TabReader{f: f}
	if strings.HasSuffix(fname, ".gz") {
		r.z, err = gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		r.in = bufio.NewReader(r.z)
	} else {
		r.in = bufio.NewReader(r.f)
	}
	return r, nil
}

// Read reads the next record
func (r *TabReader) Read() (s2.CellID, [][]byte, error) {
	cellID, val, err := r.readNext()
	if err != nil {
		return 0, nil, err
	}
	vals := [][]byte{val}

	for {
		nextID, nextVal, nextErr := r.readNext()
		if nextErr != nil {
			r.stashed.Err = nextErr
			break
		}
		if nextID != cellID {
			r.stashed.ID = nextID
			r.stashed.Val = nextVal
			break
		}
		vals = append(vals, nextVal)
	}

	return cellID, vals, nil
}

func (r *TabReader) readNext() (s2.CellID, []byte, error) {
	if r.stashed.Err != nil || r.stashed.Val != nil {
		val := r.stashed.Val
		r.stashed.Val = nil
		return r.stashed.ID, val, r.stashed.Err
	}

	line, err := r.in.ReadBytes('\n')
	if err != nil {
		return 0, nil, err
	}

	parts := bytes.SplitN(line, []byte{'\t'}, 2)
	if len(parts) != 2 {
		return 0, nil, fmt.Errorf("index: bad input %q", line)
	}

	un, err := strconv.ParseUint(string(parts[0]), 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("index: bad input %q", line)
	}

	val := parts[1]
	n, err := b64std.Decode(val, parts[1])
	if err != nil {
		return 0, nil, fmt.Errorf("index: bad input %q", line)
	}
	return s2.CellID(un), val[:n], nil
}

// Close closes the reader
func (r *TabReader) Close() error {
	var err error

	if r.z != nil {
		if e := r.z.Close(); e != nil {
			err = e
		}
	}
	if r.f != nil {
		if e := r.f.Close(); e != nil {
			err = e
		}
	}
	return err
}
