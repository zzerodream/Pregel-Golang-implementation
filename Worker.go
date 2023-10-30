/*Things to do:
	1. Initialize vertex instance, channel initialization(buffered channel)   (done)
	2. Master is the listening side for tcp connection, initialize connection.    (done)
	3. Initialize connection with other worker. (done)
	4. do we need a counter for log usage, which superstep are we in now. (still thinking)
	5. When receiving StartSuperstep, proceed superstep 
	6. After proceedsuperstep, inform the Master
	7. Master side: once receive all Finished signal from the workers, announce StartExchange. (not worker's job)
	8. Worker side received StartExchange:
		a. check if the workerChan is empty, if empty send EMPTY type message, handle incoming message
		b. else, send all the messages in workerChan, and also handle incoming message. After finished sending the messages, send Sent type message
		c. for handleincoming message, need to unmarshal json object, find the destination and write it to the messagechan under each vertex. 
		also need to change the state of the vertex.	
	9. Wait for next StatSuperstep instructions.

*/


/*Worker initialization and things to do after initialization
	1. initialize the workerChan and hold it under workerChan field, so that it can be used to initialize vertex.
	2. hardcode the IPs and reverse_IPs field for now.
	3. Vertices map[int]*Vertex should also be initialized.
	4. call EstablishMasterConnection(MasterIP string, port int) and store the returned conn value under MasterConnection field
	5. call func (w *Worker) StartListener(), start listening on the port 9999 in a new go
	6. call func (w *Worker)ConnectToClientsWithLowerID() (*net.TCPConn, error) to estabish connection with other workers with lower ID.
		store the conn under Connections map[int]net.Conn field.
		but how to handle the case where worker tries to establish connection but other worker hasn't start listening?
	7. workers call ReceiveGraphPartition(), will close once receive all partitions
	8. Then workers can use a go routine to call ReceiveFromMaster() for other instructions from master. 

*/
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
	//master IP?
}


//Establish connection with the master
func (w *Worker) EstablishMasterConnection(MasterIP string, port int)error{
	maxRetries := 3 // Number of retries
    retryDelay := 5*time.Second // Delay between retries
	addr := fmt.Sprintf("%s:%d", MasterIP, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve address: %v", err)
	}
	for retry := 0; retry < maxRetries; retry++ {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err == nil {
			w.MasterConnection = conn
			return nil
		}

		fmt.Printf("Failed to establish connection to %s, retrying...\n", addr)
		time.Sleep(retryDelay)
	}
	return fmt.Errorf("exhausted all retries, could not establish connection")
}
//establish connections with other workers
//Worker needs to listen on IP:9999
const basePort = 9999
func (w *Worker) StartListener() {
	listenAddr := fmt.Sprintf(":%d", basePort)
	listener, _ := net.Listen("tcp", listenAddr)
	defer listener.Close()

	fmt.Printf("Worker %d listening on port %d\n", w.ID, basePort)
	for {
		conn, err := listener.Accept()
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
		
		//go handleClient(conn)
	}
}
/*workers have higher ID will establish connection with workers with lower ID
let's say worker 1,2,3. 
	a. 1 will start listening on port 9999
	b. 2 will start listening on port 9999 and try to establish connection with worker 1
	c. 3 will start listening on port 9999 and try to establish connection with worker 1, 2

*/
func (w *Worker)ConnectToClientsWithLowerID() error{
	maxRetries := 3 // Number of retries
    retryDelay := 5*time.Second // Delay between retries

    for id := 1; id < w.ID; id++ {
        targetIP := w.IPs[id]
        addr := fmt.Sprintf("%s:%d", targetIP, basePort)
		tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to resolve address: %v", err)
		}
        for retry := 0; retry < maxRetries; retry++ {
            conn, err := net.DialTCP("tcp", nil, tcpAddr)
            if err == nil {
				w.mutex.Lock()
				w.Connections[id] = conn
				w.mutex.Unlock()
                return nil
            }

            fmt.Printf("Failed to establish connection to %s, retrying...\n", addr)
            time.Sleep(retryDelay)
        }
    }

    return fmt.Errorf("exhausted all retries, could not establish connection")

}

//receive graph partition from master and will terminate once receive terminate message
func (w *Worker) ReceiveGraphPartition() {
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
func extractIDAndEdges(message Message) (int, map[int]int) {
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
			edgeKey, err := strconv.Atoi(k)
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
			fmt.Println("Received instructions from the master to start next superstep\n")
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
			fmt.Println("Received instructions from the master to start exchanging message\n")
			// keep exchanging messages until receive a termination type
			//handle incoming messages buffer the messages
			go w.HandleAllIncomingMessages(terminateChan)
			//move the buffered messages to corresponding vertex
			go w.ReadAndAssignMessages(terminateChan)
			//send all outgoing messages in another go routine
			go w.HandleAllOutgoingMessages()	
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
    // seperate each data by \n
    data = append(data, '\n')
    _, err = w.MasterConnection.Write(data)
    if err != nil {
        return fmt.Errorf("failed to send message to master: %v", err)
    }

}
//send to other worker
func (w *Worker) SendMessageToWorker(workerID int, message Message) {
	// get the conn through workerID
	conn := Connections[workerID]
    data, err := json.Marshal(message)
    // seperate each data by \n
    data = append(data, '\n')
    _, err = conn.Write(data)
    if err != nil {
        return fmt.Errorf("failed to send message to master: %v", err)
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
				v.IncomingMessages = []*Message{} // Clear messages after processing
				v.state = IDLE
			}
		}(vertex)
	}
	wg.Wait()
	//Finished proceed the superstep

}
//handle all incoming messages
func (w *Worker) HandleAllIncomingMessages(terminate chan struct{}) {
	for {
        select {
        case <-terminate:
            // Terminate the loop/routine
            return
        default:
			for id, conn := range w.Connections {
				go w.handleIncomingMessagesFromConn(id, conn)
			}
        }
    }
}

//handle function for each connection
func (w *Worker) handleIncomingMessagesFromConn(id int, conn net.Conn) {
	fmt.Printf("Worker %d is handling messages from worker %d", w.ID, id)
    reader := bufio.NewReader(conn)
    for {
        // Reading a message, for instance, separated by '\n'
        line, err := reader.ReadString('\n')
        if err != nil {
			fmt.Printf("Error reading message: %v\n", err)
	        break
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
            // Terminate the loop/routine
            return
        default:
			w.mutex.Lock()
            // Copy the current queue and clear the original
            messagesToProcess := w.MessageQueue
			//clear the queue and release the lock
            w.MessageQueue = w.messageQueue[:0]
            w.mutex.Unlock()
			for _, msg := range w.MessageQueue {
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
			}else{
				//send type2 Worker has finished sending out all outgoing messages.
				FinishedSendingMessage := NewMessage(w.ID, -999, nil, 2)
				w.SendMessageToMaster(*FinishedSendingMessage)
			}
		}
	}
}

