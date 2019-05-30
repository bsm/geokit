package cellstore

import (
	"encoding/binary"
	"io"

	"github.com/bsm/extsort"
	"github.com/golang/geo/s2"
)

// SorterOptions define Sorter specific options.
type SorterOptions struct {
	// An optional temporary directory. Default: os.TempDir()
	TempDir string
}

func (o *SorterOptions) norm() *SorterOptions {
	var oo SorterOptions
	if o != nil {
		oo = *o
	}
	return &oo
}

// Sorter allows to pre-sort entries to avoid out-of-order appends to Writer instances.
type Sorter struct {
	x *extsort.Sorter
	t []byte
}

// NewSorter creates a sorter.
func NewSorter(o *SorterOptions) *Sorter {
	o = o.norm()
	return &Sorter{
		x: extsort.New(&extsort.Options{WorkDir: o.TempDir}),
	}
}

// Append appends a cell to the sorter.
func (s *Sorter) Append(cellID s2.CellID, data []byte) error {
	if !cellID.IsValid() {
		return errInvalidCellID
	}

	if sz := 8 + len(data); sz < cap(s.t) {
		s.t = s.t[:sz]
	} else {
		s.t = make([]byte, sz)
	}

	binary.BigEndian.PutUint64(s.t[0:], uint64(cellID))
	copy(s.t[8:], data)
	return s.x.Append(s.t)
}

// Sort sorts appended values and returns an iterator.
func (s *Sorter) Sort() (*SorterIterator, error) {
	iter, err := s.x.Sort()
	if err != nil {
		return nil, err
	}
	return &SorterIterator{it: iter}, nil
}

// Close closes the sorter and releases all resources.
func (s *Sorter) Close() error {
	return s.x.Close()
}

// SorterIterator iterates over sorted results
type SorterIterator struct {
	it *extsort.Iterator

	current [][]byte
	nextID  s2.CellID
	next    [][]byte
}

// NextEntry reads the next entry. This function will return io.EOF if no more entries can be read.
func (i *SorterIterator) NextEntry() (s2.CellID, [][]byte, error) {
	currentID := i.nextID
	for i.it.Next() {
		rawdata := i.it.Data()
		i.nextID = s2.CellID(binary.BigEndian.Uint64(rawdata))

		if currentID != 0 && currentID != i.nextID {
			i.next = i.push(i.next, rawdata[8:])
			break
		}
		currentID = i.nextID
		i.current = i.push(i.current, rawdata[8:])
	}

	if err := i.it.Err(); err != nil {
		return 0, nil, err
	}

	if size := len(i.current); size != 0 {
		i.current, i.next = i.next, i.current[:0]
		return currentID, i.next[:size], nil
	}

	return 0, nil, io.EOF
}

// Close closes iterator and releases resources.
func (i *SorterIterator) Close() error {
	return i.it.Close()
}

func (i *SorterIterator) push(chunks [][]byte, chunk []byte) [][]byte {
	if pos := len(chunks); pos+1 < cap(chunks) {
		chunks = chunks[:pos+1]
		chunks[pos] = append(chunks[pos][:0], chunk...)
	} else {
		cloned := make([]byte, len(chunk))
		copy(cloned, chunk)
		chunks = append(chunks, cloned)
	}
	return chunks
}
