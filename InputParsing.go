package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
)

func ParseInput(inputPath string) *Graph {
	// Read the JSON file
	data, err := ReadJSONFile("graph.json")
	if err != nil {
		fmt.Println(err)
		return &Graph{}
	}

	// Parse the JSON file as a graph
	graph, err := ParseJSONGraph(data)
	if err != nil {
		fmt.Println(err)
		return &Graph{}
	}

	// Print the graph details
	fmt.Println("Graph details:")
	fmt.Println("Number of vertices:", len(graph.Vertices))
	fmt.Println("Number of edges:", len(graph.Edges))
	for i, v := range graph.Vertices {
		fmt.Printf("Vertex %d: ID = %d, Value = %v\n", i+1, v.ID, v.Value)
	}
	for i, e := range graph.Edges {
		fmt.Printf("Edge %d: Source = %d, Target = %d, Weight = %f\n", i+1, e.Source.ID, e.Target.ID, e.Weight)
	}

	return graph
}

// temporary definition of graph
// A Vertex represents a node in a graph with a unique ID and a value
type Vertex struct {
	ID    int
	Value interface{}
	edges []Edge
}

// An Edge represents a connection between two vertices in a graph with a weight
type Edge struct {
	Source *Vertex
	Target *Vertex
	Weight float64
}

// ParseJSONGraph parses a JSON byte slice and returns a graph object
func ParseJSONGraph(data []byte) (*Graph, error) {
	var graph Graph
	err := json.Unmarshal(data, &graph)
	if err != nil {
		return nil, err
	}
	return &graph, nil
}

// A Graph represents a collection of vertices and edges
type Graph struct {
	Vertices []*Vertex
	Edges    []*Edge
}

// ReadJSONFile reads a JSON file and returns its content as a byte slice
func ReadJSONFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}

// A function to partition a slice of Vertex as a graph into k parts while minimizing edge cutting
func Partition(graph []Vertex, k int) [][]Vertex {
	// Initialize an empty slice of parts
	parts := make([][]Vertex, k)

	// Initialize an array to store the part ids of each vertex
	part := make([]int, len(graph))

	// Initialize an array to store the number of edges cut for each part
	cut := make([]int, k)

	// Initialize an array to store the total weight of edges for each part
	weight := make([]float64, k)

	// Initialize a variable to store the total number of edges cut
	totalCut := 0

	// Loop through the vertices in random order
	order := rand.Perm(len(graph))
	for _, i := range order {
		u := graph[i]
		// Find the best part for u that minimizes the edge cutting and balances the weight
		bestPart := -1
		bestCut := len(graph)
		bestWeight := 0.0

		for p := 0; p < k; p++ {
			// Count the number of edges cut and the total weight for part p
			c := 0
			w := 0.0
			for _, e := range u.edges {
				if part[e.Target.ID] != p {
					c++
				}
				w += e.Weight
			}

			// Compare the cut and weight with the current best part
			if c < bestCut || (c == bestCut && weight[p]+w < bestWeight) {
				bestPart = p
				bestCut = c
				bestWeight = weight[p] + w
			}
		}

		// Assign u to the best part and update the cut and weight arrays
		part[u.ID] = bestPart
		cut[bestPart] += bestCut
		weight[bestPart] += bestWeight
		totalCut += bestCut

	}

	// Print the part ids, cut edges, and weights for each part
	fmt.Println("The part ids for each vertex are:", part)
	fmt.Println("The number of cut edges for each part are:", cut)
	fmt.Println("The total weight of edges for each part are:", weight)
	fmt.Println("The total number of cut edges is:", totalCut)

	// Return the parts as a slice of slices of Vertex
	for i, u := range graph {
		parts[part[i]] = append(parts[part[i]], u)
	}
	return parts
}
