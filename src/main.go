package main

import "main/vertex"
import "main/message"
import "fmt"
import "time"

func main() {
	workerChan := make(chan *message.Message)

	vertex1_neighbours := map[int]int{2: 1, 3: 1}
	vertex2_neighbours := map[int]int{1: 1, 3: 3}
	vertex3_neighbours := map[int]int{2: 3, 1: 1}

	vertex1 := vertex.NewVertex(1, vertex1_neighbours, workerChan)
	vertex2 := vertex.NewVertex(2, vertex2_neighbours, workerChan)
	vertex3 := vertex.NewVertex(3, vertex3_neighbours, workerChan)

	all_vertex := []*vertex.Vertex{vertex1, vertex2, vertex3}
	
	go listenForWorkerChan(workerChan, all_vertex)
	go vertex1.SendMessagesToServer()
	go vertex2.SendMessagesToServer()
	go vertex3.SendMessagesToServer()

    <- time.After(2 * time.Second)
	for _, v := range all_vertex {
		go func(ve *vertex.Vertex) {
			ve.Compute()
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
		fmt.Printf("From %d, To %d, Content: %v\n", msg.From, msg.To, msg.Value)
		go func() {all_vertex[msg.To-1].MessageChan <- msg} ()
	}
}