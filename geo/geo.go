package geo

import (
	"encoding/binary"
	"io"
)

func binWrite(w io.Writer, v interface{}) error {
	return binary.Write(w, binary.LittleEndian, v)
}
