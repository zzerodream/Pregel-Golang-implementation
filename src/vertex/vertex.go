package vertex

import "edge"
import "message"
import "fmt"

type State int

const (
	IDLE State = iota
	ACTIVE
)

type Vertex struct {
	id int
	distance map[int]int
	state State
	edges []*edge.Edge
	CommandChan chan string
	MessageChan chan *message.Message
	workerChan chan *message.Message 
}

func NewVertex(id int, edges []*edge.Edge, workerChan chan *message.Message) *Vertex {
	vertex := &Vertex {
		id: id,
		distance: make(map[int]int),
		state: IDLE,
		edges: edges,
		CommandChan: make(chan string),
		MessageChan: make(chan *message.Message),
		workerChan: workerChan,
	}
	vertex.distance[vertex.id] = 0
	for _, neighbourEdge := range edges {
		vertex.distance[neighbourEdge.Target] = neighbourEdge.Weight
	}
	// go v.listenForCommandChan()
	// go vertex.SendMessagesToServer() 
	return vertex
}

func (v *Vertex) ListenForCommandChan() {
	for {
		select {
		case command := <-v.CommandChan:
			fmt.Printf("Vertex %d: Received command: %s\n", v.id, command)
			if command == "Start" {
				fmt.Printf("Vertex %d: Prepare Calculation\n", v.id)
				v.state = ACTIVE
				go v.compute()
			}
		}
	}
}


func (v *Vertex) UpdateState(newState State) {
	v.state = newState
}

func (v *Vertex) SendMessagesToServer() {
	for _, neighbourEdge := range v.edges {
		target := neighbourEdge.Target
		newMsg := message.NewMessage(v.id, target, v.distance)
		go func(msg *message.Message) {
			v.workerChan <- msg
		}(newMsg)
	}
}

// func (v *Vertex) compute() {
// 	fmt.Printf("Vertex %d: Calculating\n", v.id)
// 	anyChange := false
// 	for message := range v.MessageChan {
// 		fmt.Printf("Vertex %d: Dealing with %v\n", v.id, message.Payload)
// 		from := message.SenderId
// 		payload := message.Payload
// 		for target, dist := range payload {
// 			myDistance, exists := v.distance[target]
// 			if exists {
// 				if (v.distance[from] + dist) < myDistance {
// 					v.distance[target] = v.distance[from] + dist
// 					anyChange = true
// 				}
// 			} else {
// 				v.distance[target] = dist
// 				anyChange = true
// 			}
// 		}
// 		fmt.Printf("Vertex %d: Finished Dealing with %v, Distance: %v\n", v.id, message.Payload, v.distance)
// 	}
// 	fmt.Printf("Vertex %d: Clear Messages Queue. Distance: %v.\n", v.id, v.distance)
// 	if !anyChange {
// 		v.state = IDLE
// 		fmt.Printf("Vertex %d: State: %s\n", v.id, v.state)
// 	} else {
// 		fmt.Printf("Vertex %d: Sending messages back.\n", v.id, v.distance)
// 		go v.SendMessagesToServer() 
// 	}
// }

func (v *Vertex) compute() {
	fmt.Printf("Vertex %d: Calculating\n", v.id)
	anyChange := false

ReadLoop:
	for {
		select {
		case message, ok := <-v.MessageChan:
			if !ok {
				// The channel is closed, exit the loop
				break ReadLoop
			}
			fmt.Printf("Vertex %d: Dealing with %v\n", v.id, message.Payload)
			from := message.SenderId
			payload := message.Payload
			for target, dist := range payload {
				myDistance, exists := v.distance[target]
				if exists {
					if (v.distance[from] + dist) < myDistance {
						v.distance[target] = v.distance[from] + dist
						anyChange = true
					}
				} else {
					v.distance[target] = dist
					anyChange = true
				}
			}
			fmt.Printf("Vertex %d: Finished Dealing with %v, Distance: %v\n", v.id, message.Payload, v.distance)
		default:
			break ReadLoop
		}
	}
	if !anyChange {
		v.state = IDLE	
	} else {
		go v.SendMessagesToServer()
	}
	fmt.Printf("Vertex %d: State: %d\n", v.id, v.state)
}
