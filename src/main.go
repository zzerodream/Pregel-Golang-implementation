package main

import "vertex"
import "message"
import "edge"
import "fmt"
import "time"

func main() {
	workerChan := make(chan *message.Message)

	edge1 := edge.NewEdge(1, 2, 1)
	edge2 := edge.NewEdge(2, 3, 3)
	edge3 := edge.NewEdge(1, 3, 1)
	
	edge4 := edge.NewEdge(2, 1, 1)
	edge5 := edge.NewEdge(3, 2, 3)
	edge6 := edge.NewEdge(3, 1, 1)

	vertex1_neighbours := []*edge.Edge{edge1, edge3}
	vertex2_neighbours := []*edge.Edge{edge2, edge4}
	vertex3_neighbours := []*edge.Edge{edge5, edge6}

	vertex1 := vertex.NewVertex(1, vertex1_neighbours, workerChan)
	vertex2 := vertex.NewVertex(2, vertex2_neighbours, workerChan)
	vertex3 := vertex.NewVertex(3, vertex3_neighbours, workerChan)
	go vertex1.ListenForCommandChan()
	go vertex2.ListenForCommandChan()
	go vertex3.ListenForCommandChan()

	all_vertex := []*vertex.Vertex{vertex1, vertex2, vertex3}
	
	go listenForWorkerChan(workerChan, all_vertex)
	go vertex1.SendMessagesToServer()
	go vertex2.SendMessagesToServer()
	go vertex3.SendMessagesToServer()

	command := "Start"
    <- time.After(2 * time.Second)
	for _, v := range all_vertex {
		go func(ve *vertex.Vertex) {
			ve.CommandChan <- command
		}(v)
	}


	var userInput string
	_, err := fmt.Scanln(&userInput)
	if err != nil {
        fmt.Println("Error reading input:", err)
        return
    }
	fmt.Printf("Input: %s.\nProgram is exiting...\n", userInput)
}


func listenForWorkerChan(workerChan chan *message.Message, all_vertex []*vertex.Vertex) {
	for {
		msg := <- workerChan
		fmt.Printf("From %d, To %d, Content: %v\n", msg.SenderId, msg.TargetId, msg.Payload)
		go func() {all_vertex[msg.TargetId-1].MessageChan <- msg} ()
	}
}