package osmx

import (
	"errors"
	"fmt"
	"sort"

	"github.com/bsm/geokit/geo"
	osm "github.com/glaslos/go-osm"
)

// Map wraps osm.Map.
type Map struct {
	*osm.Map
	rel osm.Relation
}

// WrapMap initialises Map and sorts indexes
// for further processing.
func WrapMap(parent *osm.Map) (*Map, error) {
	if len(parent.Relations) == 0 {
		return nil, errors.New("osmx: map contains no relations")
	}

	// Get first relation that has ways
	var m *Map
	for _, rel := range parent.Relations {
		for _, mem := range rel.Members {
			if mem.Type == "way" {
				m = &Map{Map: parent, rel: rel}
				break
			}
		}
	}
	if m == nil {
		return nil, errors.New("osmx: map contains no valid relations")
	}

	sort.Slice(m.Nodes, func(i, j int) bool { return m.Nodes[i].ID < m.Nodes[j].ID })
	sort.Slice(m.Ways, func(i, j int) bool { return m.Ways[i].ID < m.Ways[j].ID })
	return m, nil
}

// CountryAlpha2 returns the ISO3166-1 alpha3 code of the Map
func (m *Map) CountryAlpha2() string {
	for _, tag := range m.rel.Tags {
		if tag.Key == "ISO3166-1:alpha2" {
			return tag.Value
		}
	}
	return ""
}

// CountryAlpha3 returns the ISO3166-1 alpha3 code of the Map
func (m *Map) CountryAlpha3() string {
	for _, tag := range m.rel.Tags {
		if tag.Key == "ISO3166-1:alpha3" {
			return tag.Value
		}
	}
	return ""
}

// Rel returns the primary relation in Map.
func (m *Map) Rel() *osm.Relation { return &m.rel }

// FindNode finds and returns a node by its ID.
func (m *Map) FindNode(id int64) (*osm.Node, error) {
	if pos := sort.Search(len(m.Nodes), func(i int) bool { return m.Nodes[i].ID >= id }); pos < len(m.Nodes) && m.Nodes[pos].ID == id {
		return &m.Nodes[pos], nil
	}
	return nil, fmt.Errorf("osmx: node #%d not found", id)
}

// FindWay finds and returns a way by its ID.
func (m *Map) FindWay(id int64) (*osm.Way, error) {
	if pos := sort.Search(len(m.Ways), func(i int) bool { return m.Ways[i].ID >= id }); pos < len(m.Ways) && m.Ways[pos].ID == id {
		if way := m.Ways[pos]; len(way.Nds) != 0 {
			return &way, nil
		}
	}
	return nil, fmt.Errorf("osmx: way #%d not found", id)
}

// GeneratePolygon parses and constructs a Polygon from the map.
func (m *Map) GeneratePolygon() (geo.Polygon, error) {
	ml, err := m.makeLineMap()
	if err != nil {
		return nil, err
	}

	lps, err := ml.Loops()
	return geo.Polygon(lps), err
}

// --------------------------------------------------------------------

func (m *Map) makeLineMap() (lineMap, error) {
	rel := m.Rel()
	res := make(lineMap, len(rel.Members))

	for _, om := range rel.Members {
		if om.Type != "way" {
			continue
		}

		ow, err := m.FindWay(om.Ref)
		if err != nil {
			return nil, err
		}

		ln := &Line{Role: om.Role, Path: make([]*osm.Node, 0, len(ow.Nds))}
		for _, nd := range ow.Nds {
			on, err := m.FindNode(nd.ID)
			if err != nil {
				return nil, err
			}
			ln.Path = append(ln.Path, on)
		}

		if ln.IsValid() {
			res[ow.ID] = ln
		}
	}
	return res, nil
}
