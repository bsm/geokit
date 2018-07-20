package cellstore

import (
	"testing"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/cellstore")
}

const seedCellID = 1317624576600000001

func coverRange(min, max s2.CellID) types.GomegaMatcher {
	return WithTransform(func(cells []s2.CellID) []s2.CellID {
		if len(cells) == 0 {
			return nil
		}
		return []s2.CellID{cells[0], cells[len(cells)-1]}
	}, Equal([]s2.CellID{min, max}))
}
