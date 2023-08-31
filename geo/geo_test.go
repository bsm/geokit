package geo

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/golang/geo/s2"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "geokit/geo")
}

var (
	colorado = []s2.Point{
		s2.PointFromLatLng(s2.LatLngFromDegrees(41, -102)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(41, -109)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(37, -109)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(37, -102)),
	}

	starshape = []s2.Point{
		s2.PointFromLatLng(s2.LatLngFromDegrees(52.8, -2.8)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(64.3, -7.7)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(54.2, -5.9)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(60.8, -13.5)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(52.4, -8.4)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(57.2, -20.0)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(49.8, -10.7)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(44.8, -26.7)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(47.9, -10.3)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(40.1, -15.8)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(45.7, -5.8)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(36.5, -4.0)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(45.6, -2.5)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(45.6, -2.5)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(35.0, 7.9)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(46.2, 1.2)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(43.5, 11.3)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(47.6, 3.3)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(48.9, 9.5)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(49.7, 3.0)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(54.3, 10.5)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(51.7, 2.1)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(59.0, 9.1)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(52.8, 1.1)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(62.5, 6.3)),
		s2.PointFromLatLng(s2.LatLngFromDegrees(52.8, -2.5)),
	}
)

type testLL struct{ Lat, Lng float64 }

func testLLFromLL(ll s2.LatLng) testLL {
	return testLL{Lat: ll.Lat.Degrees(), Lng: ll.Lng.Degrees()}
}
