package osmx

import (
	"errors"
	"fmt"
	"io"
	"sort"

	osm "github.com/glaslos/go-osm"
	"github.com/golang/geo/s2"
)

// ErrNoRelations is returned on map without relations decoding/wrapping.
var ErrNoRelations = errors.New("osmx: map contains no relations")

var errNoValidRelations = errors.New("osmx: map contains no valid relations")

// Map wraps osm.Map.
type Map struct {
	*osm.Map
	rel osm.Relation
}

// Decode decodes a Map.
func Decode(r io.Reader) (*Map, error) {
	parent, err := osm.Decode(r)
	if err != nil {
		return nil, err
	}
	return WrapMap(parent)
}

// WrapMap initialises Map and sorts indexes
// for further processing.
func WrapMap(parent *osm.Map) (*Map, error) {
	if len(parent.Relations) == 0 {
		return nil, ErrNoRelations
	}

	// Get first relation that has ways
	var m *Map
	for ri := range parent.Relations {
		rel := parent.Relations[ri]
		for mi := range rel.Members {
			mem := rel.Members[mi]
			if mem.Type == "way" {
				m = &Map{Map: parent, rel: rel}
				break
			}
		}
	}
	if m == nil {
		return nil, errNoValidRelations
	}

	sort.Slice(m.Nodes, func(i, j int) bool { return m.Nodes[i].ID < m.Nodes[j].ID })
	sort.Slice(m.Ways, func(i, j int) bool { return m.Ways[i].ID < m.Ways[j].ID })
	return m, nil
}

// Tag returns the value if a particular tag.
func (m *Map) Tag(key string) string {
	for i := range m.rel.Tags {
		if tag := m.rel.Tags[i]; tag.Key == key {
			return tag.Value
		}
	}
	return ""
}

// Rel returns the primary relation in Map.
func (m *Map) Rel() *osm.Relation {
	return &m.rel
}

// FindNode finds and returns a node by its ID.
func (m *Map) FindNode(id int64) (*osm.Node, error) {
	if pos := sort.Search(len(m.Nodes), func(i int) bool { return m.Nodes[i].ID >= id }); pos < len(m.Nodes) && m.Nodes[pos].ID == id {
		node := m.Nodes[pos]
		return &node, nil
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

// ExtractLoops extracts and generates (outer) loops from the map.
func (m *Map) ExtractLoops() ([]*s2.Loop, error) {
	ways, err := m.extractWays()
	if err != nil {
		return nil, err
	}

	loops := make([]*s2.Loop, 0, len(ways))
	for _, line := range ways {
		loop, err := line.Loop()
		if err != nil {
			return nil, err
		}

		loops = append(loops, loop)
	}
	return loops, nil
}

// --------------------------------------------------------------------

func (m *Map) extractWays() (waySlice, error) {
	ways := make(waySlice, 0, len(m.rel.Members))

	for _, osmMember := range m.rel.Members {
		if osmMember.Type != "way" {
			continue
		}

		osmWay, err := m.FindWay(osmMember.Ref)
		if err != nil {
			return nil, err
		}

		way := &wayPath{
			Role: osmMember.Role,
			Path: make([]*osm.Node, 0, len(osmWay.Nds)),
		}
		for _, nodeRef := range osmWay.Nds {
			osmNode, err := m.FindNode(nodeRef.ID)
			if err != nil {
				return nil, err
			}
			way.Path = append(way.Path, osmNode)
		}

		if way.IsValid() {
			ways = append(ways, way)
		}
	}
	return ways.Reduce(), nil
}
