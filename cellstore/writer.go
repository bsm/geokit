package cellstore

import (
	"io"

	"github.com/bsm/sntable"
)

// NewWriter wraps a writer and returns a sntable Writer.
func NewWriter(w io.Writer, o *sntable.WriterOptions) *sntable.Writer {
	return sntable.NewWriter(w, o)
}
