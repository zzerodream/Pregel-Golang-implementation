
package main
import (
    "fmt"
    "net"
    "strings"
    "sync"
    "bufio"
    "encoding/json"
    "strconv"
	"time"
)

// Worker represents a worker in the system.
type Worker struct {
	ID      int
	Vertices map[int]*Vertex 
	MessageQueue []Message //buffer the incoming message
	workerChan chan *Message //should be initialized when worker struc is initialized.
	IPs map[int]string //other worker's IP {1:ip,2:ip} worker id to ip
	reverse_IPs map[string]int //ip:1, ip:2  ip to worker id
	Connections map[int]net.Conn  // ID -> conn
	MasterConnection net.Conn  //conn object with the msater
	mutex        sync.Mutex 
	numberOfWorkers int
	MasterIP string
	MasterPort int
}


//Establish connection with the master
func (w *Worker) EstablishMasterConnection(){
	maxRetries := 3 // Number of retries
    retryDelay := 5*time.Second // Delay between retries
	addr := fmt.Sprintf("%s:%d", w.MasterIP, w.MasterPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		fmt.Printf("failed to resolve address: %v\n", err)
	}
	for retry := 0; retry < maxRetries; retry++ {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err == nil {
			w.MasterConnection = conn
			fmt.Printf("Successfully established connection with the master\n")
			return
		}

		fmt.Printf("Failed to establish connection to %s, retrying...\n", addr)
		time.Sleep(retryDelay)
	}
	fmt.Printf("exhausted all retries, could not establish connection\n")
}
//establish connections with other workers
//Worker needs to listen on IP:9999
const basePort = 9999
func (w *Worker) StartListener() {
	//if this is the worker with highest ID, won't start listening
	if w.ID == w.numberOfWorkers{
		return
	}
	listenAddr := fmt.Sprintf(":%d", basePort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
	    fmt.Printf("Worker %d Error creating listener: %v\n", w.ID, err)
	    return
	}
	defer listener.Close()
	count := 0
	fmt.Printf("Worker %d listening on port %d\n", w.ID, basePort)
	for {
		conn, err := listener.Accept()
		count += 1
		if err != nil {
			fmt.Printf("Worker %d Error accepting connection: %v\n", w.ID, err)
			continue
		}
		// maintain a map so that we can find the conn according to workerID
		remoteAddress := conn.RemoteAddr().String()
		ipAndPort := strings.Split(remoteAddress, ":")
		ip := ipAndPort[0]
		workerID := w.reverse_IPs[ip]
		w.mutex.Lock()
		w.Connections[workerID] = conn
		w.mutex.Unlock()
		if count == w.numberOfWorkers-w.ID{ //If all workers with higher ID has established connection with current worker, return.
			
			return
		}
	}
}
/*workers have higher ID will establish connection with workers with lower ID
let's say worker 1,2,3. 
	a. 1 will start listening on port 9999
	b. 2 will start listening on port 9999 and try to establish connection with worker 1
	c. 3 will start listening on port 9999 and try to establish connection with worker 1, 2

*/
func (w *Worker)ConnectToWorkerssWithLowerID(){
	//if this is the worker with lowest ID 1, just return.c
	if w.ID == 1{
		return
	}
	maxRetries := 3 // Number of retries
    retryDelay := 2*time.Second // Delay between retries
	time.Sleep(2*time.Second) //wait a few seconds before trying to establish connection, ensure that all workers have start listening
    count := 0
	for id := 1; id < w.ID; id++ {
        targetIP := w.IPs[id]
        addr := fmt.Sprintf("%s:%d", targetIP, basePort)
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
        for retry := 0; retry < maxRetries; retry++ {
            conn, err := net.DialTCP("tcp", nil, tcpAddr)
            if err == nil {
				w.mutex.Lock()
				count += 1
				w.Connections[id] = conn
				w.mutex.Unlock()
				if count == w.ID-1{
					return
				}else{
					continue
				}
            }
            fmt.Printf("Failed to establish connection to %d, retrying...\n", id)
            time.Sleep(retryDelay)
        }
		fmt.Printf("exhausted all retries, could not establish connection to %d\n",id)
    }
	fmt.Printf("Successfully established connection with workers with lower ID\n")
}

//receive graph partition from master and will terminate once receive terminate message
func (w *Worker) ReceiveGraphPartition() {
	fmt.Println("Start receving graph partition from the master")
	reader := bufio.NewReader(w.MasterConnection)
	for {
		//assume that every message from master is sepetated by '\n'
	    line, err := reader.ReadString('\n')
	    if err != nil {
	        fmt.Printf("Error reading message: %v\n", err)
	        break
	    }
		//decode the json and create the message object
	    var message Message
	    err = json.Unmarshal([]byte(line), &message)
	    if err != nil {
	        fmt.Printf("Error unmarshalling message: %v\n", err)
	        continue
	    } 
		// if receive a message that indicate that server has sent all vertices to the worker. 
		if message.Type == 7 {
			fmt.Println("Received all partitions from the master")
			//maybe wait a few seconds to ensure 
			break
		}
		//receive graph partition, we will initialize vertex instance.
		if message.Type == 6{
			VertexID, edges, err := extractIDAndEdges(message)
			fmt.Printf("Received messaged with vertexID %d, and edges %v\n",VertexID, edges)
			if err != nil {
				fmt.Printf("Error extracting data: %v\n", err)
				continue
			}
			//now we have the ID. edges and WorkerChannel, initialize the vertex struc
			v := NewVertex(VertexID, edges, w.workerChan)
			//hold the vertices info under worker.Vertices
			w.Vertices[VertexID] = v
		}
	}
}
// extract information from master
// recall we define func NewVertex(id int, edges map[int]int, workerChan chan *Message) *Vertex
func extractIDAndEdges(message Message) (int, map[int]int, error) {
	valueMap, ok := message.Value.(map[string]interface{})
	if !ok {
		return 0, nil, fmt.Errorf("Value is not a map")
	}
	//// JSON numbers are parsed as float64 by default, so we need to convert it to int
	idValue, ok := valueMap["ID"].(float64)
	if !ok {
		return 0, nil, fmt.Errorf("ID is not a valid number")
	}
	ID := int(idValue)

	convertedEdges := make(map[int]int)
	//cause the edges in message is in format like this "edges":{"2":3, "3":5}
	edges, ok := valueMap["edges"].(map[string]interface{})
	if ok {
		for k, v := range edges {
			//convert key to int
			edgeKey, _ := strconv.Atoi(k)
			//assert that value is float type
			edgeValue, ok := v.(float64)
			if ok {
				convertedEdges[edgeKey] = int(edgeValue)
			}
		}
	}

	return ID, convertedEdges, nil
}

//receive other messages from master like startsuperstep, startexchange
func (w *Worker) ReceiveFromMaster(){
	reader := bufio.NewReader(w.MasterConnection)
	var terminateChan chan struct{}
	for {
		//assume that every message from master is sepetated by '\n'
	    line, err := reader.ReadString('\n')
	    if err != nil {
	        fmt.Printf("Error reading message: %v\n", err)
	        break
	    }
		//decode the json and create the message object
	    var message Message
	    err = json.Unmarshal([]byte(line), &message)
	    if err != nil {
	        fmt.Printf("Error unmarshalling message: %v\n", err)
	        continue
	    } 
		switch message.Type{
			// StartSuperstep message
		case 0:
			fmt.Printf("Received instructions from the master to start next superstep\n")
			//cal ProceedSuperstep()
			w.ProceedSuperstep()
			// After proceedsuperstep, inform the Master, mater ID field, computeFinished, type 1
			FinishedSuperstepMessage := NewMessage(w.ID, -999, nil, 1)
			go w.SendMessageToMaster(*FinishedSuperstepMessage)  //use fo routine or not?
		case 4:// StartExchange message from Master
			// just in case the channel is not closed, but unlikely to happen
			if terminateChan != nil {
				close(terminateChan)
			}
			terminateChan = make(chan struct{})
			fmt.Printf("Received instructions from the master to start exchanging message\n")
			// keep exchanging messages until receive a termination type
			//send all outgoing messages in another go routine
			go w.HandleAllOutgoingMessages()
			//handle incoming messages buffer the messages
			go w.HandleAllIncomingMessages(terminateChan)
			//move the buffered messages to corresponding vertex
			go w.ReadAndAssignMessages(terminateChan)
	
		case 8://exchange stopped 
			if terminateChan != nil {
				close(terminateChan)
				terminateChan = nil
			}
		}
	}
}

//send message to Master
func (w *Worker) SendMessageToMaster(message Message) {
    data, err := json.Marshal(message)
    if err != nil {
        fmt.Printf("failed to marshal the message: %v\n", err)
    }	
    // seperate each data by \n
    data = append(data, '\n')
    _, err = w.MasterConnection.Write(data)
    if err != nil {
        fmt.Printf("failed to send message to master: %v\n", err)
    }

}
//send to other worker
func (w *Worker) SendMessageToWorker(workerID int, message Message) {
	// get the conn through workerID
	conn := w.Connections[workerID]
    data, err := json.Marshal(message)
	if err != nil {
        fmt.Printf("failed to marshal the message: %v\n", err)
    }	
    // seperate each data by \n
    data = append(data, '\n')
    _, err = conn.Write(data)
    if err != nil {
        fmt.Printf("failed to send message to other worker: %v\n", err)
    }
}
// ProceedSuperstep processes one superstep for all vertices.
func (w *Worker) ProceedSuperstep() {
	var wg sync.WaitGroup
	for _, vertex := range w.Vertices {
		wg.Add(1)
		go func(v *Vertex) {
			defer wg.Done()
			if v.state == ACTIVE {
				v.Compute()
				//v.IncomingMessages = []*Message{}
				v.state = IDLE
			}
		}(vertex)
	}
	wg.Wait()
	//Finished proceed the superstep
}
//handle all incoming messages
func (w *Worker) HandleAllIncomingMessages(terminate chan struct{}) {
	activeConns := make(map[int]bool) // to avoid multiple goroutines on the same connection
	for id, conn := range w.Connections {
		// if we haven't created a go routine to handle this conn, create one. else do nothing
		if !activeConns[id] { 
			activeConns[id] = true
			go w.handleIncomingMessagesFromConn(id, conn, terminate)
		}
	}
	// wait for the termination signal
	<-terminate
	return
}

//handle function for each connection
func (w *Worker) handleIncomingMessagesFromConn(id int, conn net.Conn, terminate chan struct{}) {
	fmt.Printf("Worker %d is handling messages from worker %d\n", w.ID, id)
    reader := bufio.NewReader(conn)
    for {
		select{
		case <- terminate:
			return
		default:
		}
		// a timeout mechanism so that it won't be block if there's no message to read and keep looping, so that we can check the termination signal correctly
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
        // Reading a message, for instance, separated by '\n'
        line, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// It's a timeout error; continue looping to check the terminate signal
				continue
			} else {
				// It's some other error
				fmt.Printf("Error reading message: %v\n", err)
				return
			}
		}
        // Process the received message here, todo: unmarshal the message and buffer the message in the MessageQueue []Message
		var message Message
	    err = json.Unmarshal([]byte(line), &message)
		w.EnqueueMessage(message)	
    }
}
//buffer the incoming message in the MessageQueue
func (w *Worker) EnqueueMessage(message Message) {
	//need lock here cause we may write and read to the MessageQueue at the same time 
	w.mutex.Lock()
	w.MessageQueue = append(w.MessageQueue, message)
	w.mutex.Unlock()
}
//read from MessageQueue and assign it to correct vertex, update vertex state
func (w *Worker) ReadAndAssignMessages(terminate chan struct{})  {
	for {
        select {
        case <-terminate:
            // exit
            return
        default:
        }
		w.mutex.Lock()
		// Copy the current queue and clear the original
		messagesToProcess := w.MessageQueue
		//clear the queue and release the lock
		w.MessageQueue = w.MessageQueue[:0]
		w.mutex.Unlock()
		for _, msg := range messagesToProcess {
			//for log info
			fmt.Printf("Reading message from %d to %d of type %d\n", msg.From, msg.To, msg.Type)
			//assign to correct vertex
			v := w.Vertices[msg.To]
			v.IncomingMessages = append(v.IncomingMessages, msg)
			//update the vertex state
			v.UpdateState(ACTIVE)
		}
    }	
}
//handle all outgoing messages in workChan
func (w *Worker) HandleAllOutgoingMessages() {
	sendMessage := false
	var wg sync.WaitGroup
	for {
		select {
		case msg, ok := <-w.workerChan:
			if !ok {
				// channel has been closed
				return
			}
			// process the message.
			if !sendMessage{
				sendMessage = true
			}
			wg.Add(1)
			//channel passed the pointer instead of Message struc itself.
			go func(m *Message) {
				//todo: send messages to corresponding worker
				To := m.To
				workerID := To%w.numberOfWorkers
				w.SendMessageToWorker(workerID, *m)
				defer wg.Done()
			}(msg)
		default:
			wg.Wait()
			if !sendMessage{
				// At the very beginning, no message in the channel, send message to master. Type 3.
				EmptyMessage := NewMessage(w.ID, -999, nil, 3)
				w.SendMessageToMaster(*EmptyMessage)
				return
			}else{
				//send type2 Worker has finished sending out all outgoing messages.
				FinishedSendingMessage := NewMessage(w.ID, -999, nil, 2)
				w.SendMessageToMaster(*FinishedSendingMessage)
				return
			}
		}
	}
}

//
func NewWorker() *Worker{
	return &Worker{
		ID:             1,
		Vertices:       make(map[int]*Vertex),
		MessageQueue:   []Message{},
		workerChan:     make(chan *Message,100),
		IPs:            make(map[int]string),
		reverse_IPs:    make(map[string]int),
		Connections:    make(map[int]net.Conn),
		mutex:          sync.Mutex{},
		numberOfWorkers:  5, // set number of workers,
		MasterIP: "localhost", //set the IP of the msater
		MasterPort: 8080, //set port number
	}
}
// call all necessary functions for the worker
func (w *Worker) Run(){
	var wg sync.WaitGroup
	//Establish connection with master
	w.EstablishMasterConnection()  //not using go routine cause I want to make sure the connection is establish before proceeding to next step
	//while workers are trying to e currently will keep running and not killedstablish connection between each other, we can start receving partitions from the master
	wg.Add(1)
	go func(){
		w.ReceiveGraphPartition()
		wg.Done()
	}()
	//Start listener and try to establish connection with other workers
	wg.Add(1)
	go func(){
		w.StartListener()  // Begin listening for incoming connections in a new go routine, will stop once all connections are established
		wg.Done()
	}()
	wg.Add(1)
	go func(){
		w.ConnectToWorkerssWithLowerID()
		wg.Done()
	}()
	// only proceed after receving all partitions and establish all connections
	wg.Wait()
	//continue receiving instructions from master.
	w.ReceiveFromMaster()

}

