package cellstore

import (
	"errors"
)

var (
	errInvalidCellID = errors.New("cellstore: invalid cell ID")
	errReleased      = errors.New("cellstore: already released")
)
