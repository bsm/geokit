package geo

import (
	"testing"

	"github.com/golang/geo/s2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/geo")
}

// Four corners of Colorado, USA
var (
	sw = s2.LatLngFromDegrees(37, -109)
	se = s2.LatLngFromDegrees(37, -102)
	ne = s2.LatLngFromDegrees(41, -102)
	nw = s2.LatLngFromDegrees(41, -109)
)
