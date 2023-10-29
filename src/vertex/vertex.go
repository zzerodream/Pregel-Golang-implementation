package vertex

import "main/message"
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
	edges map[int]int
	MessageChan chan *message.Message
	workerChan chan *message.Message 
}

func NewVertex(id int, edges map[int]int, workerChan chan *message.Message) *Vertex {
	vertex := &Vertex {
		id: id,
		distance: make(map[int]int),
		state: ACTIVE,
		edges: edges,
		MessageChan: make(chan *message.Message),
		workerChan: workerChan,
	}
	vertex.distance[vertex.id] = 0
	for to, weight := range edges {
		vertex.distance[to] = weight
	}
	// go vertex.SendMessagesToServer() 
	return vertex
}

func (v *Vertex) UpdateState(newState State) {
	v.state = newState
}

func (v *Vertex) SendMessagesToServer() {
	for neighbour, _ := range v.edges {
		newMsg := message.NewMessage(v.id, neighbour, v.distance, 5)
		go func(msg *message.Message) {
			v.workerChan <- msg
		}(newMsg)
	}
}

func (v *Vertex) Compute() {
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
			fmt.Printf("Vertex %d: Dealing with %v\n", v.id, message.Value)
			from := message.From
			payload := message.Value.(map[int]int)
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
			fmt.Printf("Vertex %d: Finished Dealing with %v, Distance: %v\n", v.id, message.Value, v.distance)
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
