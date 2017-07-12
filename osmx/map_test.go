package osmx

import (
	osm "github.com/glaslos/go-osm"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

	It("should retrieve alpha2", func() {
		subject = &Map{
			rel: osm.Relation{
				Tags: []osm.Tag{
					{Key: "ISO3166-1:alpha2", Value: "GB"},
				},
			},
		}
		Expect(subject.CountryAlpha2()).To(Equal("GB"))
	})

	It("should retrieve alpha3", func() {
		subject = &Map{
			rel: osm.Relation{
				Tags: []osm.Tag{
					{Key: "ISO3166-1:alpha3", Value: "GBR"},
				},
			},
		}
		Expect(subject.CountryAlpha3()).To(Equal("GBR"))
	})
})
