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
})
