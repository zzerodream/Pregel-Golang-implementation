package main


import "fmt"
import "math"
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
	Value float64
	//distance map[int]int
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
		//distance: make(map[int]int),
		//state: ACTIVE,
		Value: math.Inf(1),
		state: IDLE,
		edges: edges,
		workerChan: workerChan,
	}
	// distance to itself is 0
	vertex.Value = math.Inf(1)
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
		newMsg := &Message{
			From:  v.id,
			To:    neighbour,
			Value: v.Value,
			Type:  5,
		}
		go func(msg *Message) {
			v.workerChan <- msg
		}(newMsg)
	}
}
//compute function.
func (v *Vertex) Compute() {
	fmt.Printf("Vertex %d: Calculating\n", v.id)
	updated := false // Track if the vertex's Value is updated in this superstep
    for _, msg := range v.IncomingMessages {
		fmt.Printf("Vertex %d: Dealing with %v\n", v.id, msg.Value)
		if value, ok := msg.Value.(float64); ok {
			potentialDistance := value + float64(v.edges[msg.From])
			if potentialDistance < v.Value {
				fmt.Println("updating distance!")
				v.Value = potentialDistance
				updated = true
			}
		}
    }
    if updated {
        v.SendMessageToWorker() // Assume some function workerIDFor to get the correct worker ID
		fmt.Printf("Vertex %d has updated value to neighbors\n", v.id)
	}
	v.UpdateState(IDLE)
	fmt.Printf("Vertex %d: State: %d\n", v.id, v.state)
	return
}