package edge

type Edge struct {
	Source int
	Target int
	Weight int	
}

func NewEdge(source int, target int, weight int) *Edge {
	edge := &Edge {
		Source: source,
		Target: target,
		Weight: weight,
	}
	return edge
}