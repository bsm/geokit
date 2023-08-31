package osmx

import (
	"compress/gzip"
	"os"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s2"
)

var _ = Describe("Map", func() {
	var subject *Map

	It("should require at least one relation to wrap", func() {
		_, err := WrapMap(new(osm.Map))
		Expect(err).To(MatchError(`osmx: map contains no relations`))
	})

	It("should require a relation with ways", func() {
		_, err := WrapMap(&osm.Map{
			Relations: []osm.Relation{
				{Members: []osm.Member{{Type: "notway"}}},
				{Members: []osm.Member{{Type: "alsonotway"}}},
			},
		})
		Expect(err).To(MatchError(`osmx: map contains no valid relations`))
	})

	It("should wrap the relation with way members", func() {
		var err error
		subject, err = WrapMap(&osm.Map{
			Relations: []osm.Relation{
				{Members: []osm.Member{{Type: "notway"}}},
				{Members: []osm.Member{{Type: "way"}}},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(subject.Rel()).To(Equal(&osm.Relation{
			Members: []osm.Member{{Type: "way"}},
		}))
	})

	It("should retrieve tag", func() {
		subject = &Map{
			rel: osm.Relation{
				Tags: []osm.Tag{
					{Key: "ISO3166-1:alpha2", Value: "GB"},
				},
			},
		}
		Expect(subject.Tag("ISO3166-1:alpha2")).To(Equal("GB"))
		Expect(subject.Tag("notfound")).To(Equal(""))
	})

	It("should extract loops", func() {
		Expect(extractLoops("testdata/AD.osm.gz")).To(HaveLen(1))
		Expect(extractLoops("testdata/AG.osm.gz")).To(HaveLen(4))
	})
})

func extractLoops(fname string) ([]*s2.Loop, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer z.Close()

	m, err := Decode(z)
	if err != nil {
		return nil, err
	}

	return m.ExtractLoops()
}
