package cellstore

// BPos returns the block position.
func (i *SectionIterator) BPos() int { return i.b.Pos() }

// SPos returns the section position.
func (i *SectionIterator) SPos() int { return i.s.Pos() }

// Sort sorts entries by distance.
func (n *NearbyRS) Sort() { n.sort() }
