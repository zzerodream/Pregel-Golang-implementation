package main


import "fmt"
//define state, 0 is IDLE and 1 is ACTIVE
type State int

const (
	IDLE State = iota //0
	ACTIVE //1
)
/*Things modified or discussed
	1. for MessageChan, use a slice of Message instead of channel, and rename it as IncomingMessages
	2. When initializing the vertex, the initial state is IDLE. 
	3. initializing the channel will be done at the worker side.
	4. All the vertices share the same MessageChan with the worker.
	5. Rename SendMessageToServer as SendMessageToWorker

*/
type Vertex struct {
	id int
	distance map[int]int
	state State
	edges map[int]int
	//MessageChan chan *message.Message
	IncomingMessages []Message  //for incoming message
	//workerChan chan *message.Message
	workerChan chan *Message //for outgoing messages, should be a buffered channel!
}

func NewVertex(id int, edges map[int]int, workerChan chan *Message) *Vertex {
	//create the vertex and return the address of it.
	vertex := &Vertex {
		id: id,
		distance: make(map[int]int),
		//state: ACTIVE,
		state: IDLE,
		edges: edges,
		workerChan: workerChan,
	}
	// distance to itself is 0
	vertex.distance[vertex.id] = 0
	// from the provided edges values, initialize the distance map.
	for to, weight := range edges {
		vertex.distance[to] = weight
	}
	// go vertex.SendMessagesToWorker() 
	return vertex
}
//update the state of vertex
func (v *Vertex) UpdateState(newState State) {
	v.state = newState
}
//send all outgoing messages to Worker.
func (v *Vertex) SendMessageToWorker() {
	for neighbour, _ := range v.edges {
		newMsg := NewMessage(v.id, neighbour, v.distance, 5)
		go func(msg *Message) {
			v.workerChan <- msg
		}(newMsg)
	}
}
//compute function.
func (v *Vertex) Compute() {
	fmt.Printf("Vertex %d: Calculating\n", v.id)
	anyChange := false

//ReadLoop:
//	for {
//		select {
//		case message, ok := <-v.MessageChan:
//			if !ok {
//				// The channel is closed, exit the loop
//				break ReadLoop
//			}
//			//modify the log a bit.
//			fmt.Printf("Vertex %d: Dealing with %v\n", v.id, message.Value)
//			from := message.From
//			payload := message.Value.(map[int]int)
//			for target, dist := range payload {
//				myDistance, exists := v.distance[target]
//				if exists {
//					if (v.distance[from] + dist) < myDistance {
//						v.distance[target] = v.distance[from] + dist
//						anyChange = true
//					}
//				} else {
//					v.distance[target] = dist
//					anyChange = true
//				}
//			}
//			fmt.Printf("Vertex %d: Finished Dealing with %v, Distance: %v\n", v.id, message.Value, v.distance)
//		default:
//			break ReadLoop
//		}
//	}
	for len(v.IncomingMessages) > 0 {
		// Take the first message from the slice
		message := v.IncomingMessages[0]
	
		// Process the message (this part remains unchanged)
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
	
		// Remove the processed message from the slice
		v.IncomingMessages = v.IncomingMessages[1:]
	}
	//should this state change done in worker side?	
	//v.state = IDLE
	if anyChange {
		go v.SendMessageToWorker()
	}
	fmt.Printf("Vertex %d: State: %d\n", v.id, v.state)
}