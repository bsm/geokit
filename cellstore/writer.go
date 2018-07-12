package cellstore

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/geo/s2"
	"github.com/golang/snappy"
)

// Writer represents a cellstore Writer
type Writer struct {
	w io.Writer
	o Options

	last   s2.CellID // the last cellID
	offset int64     // the current offset

	buf   []byte
	snp   []byte
	tmp   []byte
	index []blockInfo
}

// NewWriter wraps a writer and returns a cellstore Writer
func NewWriter(w io.Writer, o *Options) *Writer {
	var opts Options
	if o != nil {
		opts = *o
	}
	opts.norm()

	return &Writer{
		w:   w,
		o:   opts,
		tmp: make([]byte, 8+binary.MaxVarintLen64),
	}
}

// Append appends a cell to the store.
func (w *Writer) Append(cellID s2.CellID, data []byte) error {
	if w.tmp == nil {
		return errClosed
	}
	if !cellID.IsValid() {
		return errInvalidCellID
	} else if w.last >= cellID {
		return fmt.Errorf("cellstore: attempted an out-of-order append, %v must be > %v", cellID, w.last)
	}

	binary.BigEndian.PutUint64(w.tmp, uint64(cellID))
	n := binary.PutUvarint(w.tmp[8:], uint64(len(data))) + 8

	if len(w.buf)+len(data)+n > w.o.BlockSize {
		if err := w.flush(); err != nil {
			return err
		}
	}

	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, data...)
	w.last = cellID
	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	if w.tmp == nil {
		return errClosed
	}
	if err := w.flush(); err != nil {
		return err
	}

	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return err
	}

	flagsOffset := w.offset
	if err := w.writeFlags(); err != nil {
		return err
	}

	if err := w.writeFooter(indexOffset, flagsOffset); err != nil {
		return err
	}
	w.tmp = nil
	return nil
}

func (w *Writer) writeIndex() error {
	for _, ent := range w.index {
		binary.BigEndian.PutUint64(w.tmp[0:], uint64(ent.MaxCellID))
		n := binary.PutUvarint(w.tmp[8:], uint64(ent.Offset)) + 8
		if err := w.writeRaw(w.tmp[:n], NoCompression); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeFlags() error {
	w.tmp[0] = flagCompression
	w.tmp[1] = byte(w.o.Compression)
	if err := w.writeRaw(w.tmp[:2], NoCompression); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeFooter(indexOffset, flagsOffset int64) error {
	binary.BigEndian.PutUint64(w.tmp[0:], uint64(indexOffset))
	binary.BigEndian.PutUint64(w.tmp[8:], uint64(flagsOffset))
	if err := w.writeRaw(w.tmp[:16], NoCompression); err != nil {
		return err
	}
	if err := w.writeRaw(magic, NoCompression); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeRaw(p []byte, c Compression) error {
	if c == SnappyCompression {
		p = snappy.Encode(w.snp[:cap(w.snp)], p)
		w.snp = p
	}

	n, err := w.w.Write(p)
	w.offset += int64(n)
	return err
}

func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	w.index = append(w.index, blockInfo{
		MaxCellID: w.last,
		Offset:    w.offset,
	})

	err := w.writeRaw(w.buf, w.o.Compression)
	w.buf = w.buf[:0]
	return err
}
