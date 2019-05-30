package cellstore

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bsm/extsort"
	"github.com/golang/geo/s2"
	"github.com/golang/snappy"
)

// Writer represents a cellstore Writer
type Writer struct {
	w io.Writer
	o *Options

	block blockInfo // the current block info
	blen  int       // the number of entries in the current block
	soffs []int     // section offsets in the current block

	buf []byte // plain buffer
	snp []byte // snappy  buffer
	tmp []byte // scratch buffer

	index []blockInfo
}

// NewWriter wraps a writer and returns a cellstore Writer
func NewWriter(w io.Writer, o *Options) *Writer {
	return &Writer{
		w:   w,
		o:   o.norm(),
		tmp: make([]byte, 2*binary.MaxVarintLen64),
	}
}

// Append appends a cell to the store.
func (w *Writer) Append(cellID s2.CellID, data []byte) error {
	if w.tmp == nil {
		return errClosed
	}
	if !cellID.IsValid() {
		return errInvalidCellID
	} else if w.block.MaxCellID >= cellID {
		return fmt.Errorf("cellstore: attempted an out-of-order append, %v must be > %v", cellID, w.block.MaxCellID)
	}

	if len(w.buf) != 0 && len(w.buf)+len(data)+2*binary.MaxVarintLen64 > w.o.BlockSize {
		if err := w.flush(); err != nil {
			return err
		}
	}

	key := cellID
	if w.blen%w.o.SectionSize == 0 { // new section?
		w.soffs = append(w.soffs, len(w.buf))
	} else {
		key -= w.block.MaxCellID // apply delta-encoding
	}

	n := binary.PutUvarint(w.tmp[0:], uint64(key))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(data)))
	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, data...)

	w.blen++
	w.block.MaxCellID = cellID

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

	indexOffset := w.block.Offset
	if err := w.writeIndex(); err != nil {
		return err
	}

	if err := w.writeFooter(indexOffset); err != nil {
		return err
	}
	w.tmp = nil
	return nil
}

func (w *Writer) writeIndex() error {
	var prev blockInfo

	for i, ent := range w.index {
		cid := ent.MaxCellID
		off := ent.Offset
		if i > 0 { // delta-encode
			cid -= prev.MaxCellID
			off -= prev.Offset
		}
		prev = ent

		n := binary.PutUvarint(w.tmp[0:], uint64(cid))
		n += binary.PutUvarint(w.tmp[n:], uint64(off))

		if err := w.writeRaw(w.tmp[:n]); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeFooter(indexOffset int64) error {
	binary.LittleEndian.PutUint64(w.tmp[0:], uint64(indexOffset))
	if err := w.writeRaw(w.tmp[:8]); err != nil {
		return err
	}
	if err := w.writeRaw(magic); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeRaw(p []byte) error {
	n, err := w.w.Write(p)
	w.block.Offset += int64(n)
	return err
}

func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	for _, o := range w.soffs {
		if o > 0 {
			binary.LittleEndian.PutUint32(w.tmp, uint32(o))
			w.buf = append(w.buf, w.tmp[:4]...)
		}
	}
	binary.LittleEndian.PutUint32(w.tmp, uint32(len(w.soffs)))
	w.buf = append(w.buf, w.tmp[:4]...)

	var block []byte
	switch w.o.Compression {
	case SnappyCompression:
		w.snp = snappy.Encode(w.snp[:cap(w.snp)], w.buf)
		if len(w.snp) < len(w.buf)-len(w.buf)/4 {
			block = append(w.snp, blockSnappyCompression)
		} else {
			block = append(w.buf, blockNoCompression)
		}
	default:
		block = append(w.buf, blockNoCompression)
	}

	w.index = append(w.index, w.block)
	w.buf = w.buf[:0]
	w.soffs = w.soffs[:0]
	w.blen = 0

	return w.writeRaw(block)
}

// SortWriter supports out-of-order appends but is significantly slower than
// standard Writer.
type SortWriter struct {
	w io.Writer
	x *extsort.Sorter
	o *Options

	scratch []byte
}

// NewSortWriter wraps a writer.
func NewSortWriter(w io.Writer, o *Options) *SortWriter {
	o = o.norm()

	return &SortWriter{
		w: w,
		x: extsort.New(&extsort.Options{WorkDir: o.TempDir}),
		o: o,
	}
}

// Append appends a cell to the store.
// Cells can be added to the writer in any order but must not contain duplicated.
// WARNING: duplicate cell data will be simply ignored.
func (w *SortWriter) Append(cellID s2.CellID, data []byte) error {
	if !cellID.IsValid() {
		return errInvalidCellID
	}

	chain := w.needScratch(8 + len(data))
	binary.BigEndian.PutUint64(chain[0:], uint64(cellID))
	copy(chain[8:], data)
	return w.x.Append(chain)
}

// Close closes the writer.
func (w *SortWriter) Close() error {
	defer w.x.Close()

	iter, err := w.x.Sort()
	if err != nil {
		return err
	}
	defer iter.Close()

	ww := NewWriter(w.w, w.o)
	prevID := s2.SentinelCellID
	for iter.Next() {
		chain := iter.Data()
		cellID := s2.CellID(binary.BigEndian.Uint64(chain))
		if cellID != prevID {
			data := w.needScratch(len(chain) - 8)
			copy(data, chain[8:])

			if err := ww.Append(cellID, data); err != nil {
				return err
			}
		}
		prevID = cellID
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if err := ww.Close(); err != nil {
		return err
	}
	return w.x.Close()
}

func (w *SortWriter) needScratch(sz int) []byte {
	if sz < cap(w.scratch) {
		w.scratch = w.scratch[:sz]
	} else {
		w.scratch = make([]byte, sz)
	}
	return w.scratch
}
