package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Define the structure that matches the JSON format
type Node struct {
	ID    int            `json:"ID"`
	Edges map[string]int `json:"edges"`
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

// ParseJSONGraph parses a JSON byte slice and returns a map matches the string ID with its Node structure
func ParseJSONGraph(data []byte) (map[string]Node, error) {
	nodes := make(map[string]Node)

	// Decode the JSON content into the map
	err := json.Unmarshal(data, &nodes)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return nil, err
	}
	return nodes, nil
}

func ParseInput(inputPath string) map[string]Node {
	// Open the JSON file
	data, err := ReadJSONFile(inputPath)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	nodes, err := ParseJSONGraph(data)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	return nodes
}

// A function to partition a slice of Vertex as a map into k machines by mod
func Partition(nodes map[string]Node, k int) []map[string]Node {
	parts := make([]map[string]Node, k)
	for i := 0; i < k; i++ {
		parts[i] = make(map[string]Node)
	}
	for id, node := range nodes {
		partNo := (node.ID-1) % k
		parts[partNo][id] = node
	}
	return parts
}
