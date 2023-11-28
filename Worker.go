
package main
import (
    "fmt"
    "net"
    "sync"
    "bufio"
    "encoding/json"
    "strconv"
	"time"
	"os"
	"math"
	"io/ioutil"
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
	MasterConnection map[int]net.Conn  //conn object with the msater
	mutex        sync.Mutex 
	numberOfWorkers int
	// MasterIP string
	// MasterPort int
	MasterAddr map[int]string
	sourceVertex int
	superstep int
	serverData map[string]interface{}
	aliveNodes []int
	currentMaster int
	isRecovered bool
}

func (w *Worker) GetServerData() {
	fileContent, err := ioutil.ReadFile("ServerAddress.json")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// Unmarshal the JSON data into the map
	err = json.Unmarshal(fileContent, &w.serverData)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return
	}

	workerData, _ := w.serverData["Worker"].([]interface{})
	masterData, _ := w.serverData["Master"].([]interface{})
	w.numberOfWorkers = len(workerData)
	for i:=1; i<=w.numberOfWorkers; i++ {
		w.aliveNodes = append(w.aliveNodes, i)
	}
	
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		addr := server["ip"].(string) + server["internalPort"].(string)
		workerId := int(server["id"].(float64))
		w.IPs[workerId] = addr
		w.reverse_IPs[addr] = workerId
	}

	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		addr := server["ip"].(string) + server["externalPort"].(string)
		masterId := int(server["id"].(float64))
		w.MasterAddr[masterId] = addr
	}
}


//Establish connection with the master
func (w *Worker) EstablishMasterConnection(){
	fmt.Printf("Master conn: %v\n", w.MasterAddr)
	for k, v := range(w.MasterAddr) {
		maxRetries := 3 // Number of retries
		retryDelay := 5*time.Second // Delay between retries
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", v)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localTCPAddr, err := net.ResolveTCPAddr("tcp", w.GetMyExternalAddr(k))
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
		fmt.Printf("localAddr: %s\n", localTCPAddr.String())
		for retry := 0; retry < maxRetries; retry++ {
			d := net.Dialer{LocalAddr: localTCPAddr}
			conn, err := d.Dial("tcp", remoteTCPAddr.String())
			// conn, err := net.DialTCP("tcp", localAddr, tcpAddr)
			if err == nil {
				w.MasterConnection[k] = conn
				fmt.Printf("Successfully established connection with the master\n")
				break
			}

			fmt.Printf("Failed to establish connection to %s - %v, retrying...\n", v, err)
			time.Sleep(retryDelay)
		}
	}
}

func (w *Worker) GetMyExternalAddrBackup(k int) string {
	addr := "127.0.0.1:3030"
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) == w.ID {
			addrList := server["externalBackup"].(map[string]interface{})
			addr = server["ip"].(string) + addrList[strconv.Itoa(k)].(string)
		}
	}
	return addr
}

//Establish connection with the master
func (w *Worker) EstablishMasterConnectionBackup(){
	fmt.Printf("Master conn: %v\n", w.MasterAddr)
	for k, v := range(w.MasterAddr) {
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", v)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localTCPAddr, err := net.ResolveTCPAddr("tcp", w.GetMyExternalAddrBackup(k))
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
		fmt.Printf("localAddr: %s\n", localTCPAddr.String())

		d := net.Dialer{LocalAddr: localTCPAddr}
		conn, err := d.Dial("tcp", remoteTCPAddr.String())
		// conn, err := net.DialTCP("tcp", localAddr, tcpAddr)
		if err == nil {
			w.MasterConnection[k] = conn
			fmt.Printf("Successfully established connection with the master\n")
		} else {
			fmt.Printf("Failed to establish connection to %s - %v\n", v, err)
		}
	}
	_, exists := w.MasterConnection[w.currentMaster]
	if !exists {
		curBiggest := 0
		for key := range w.MasterConnection {
			if key > curBiggest {
				curBiggest = key
			} 
		}
		w.currentMaster = curBiggest
	}
}

//Establish connection with the master
func (w *Worker) ConnectToWorkers(){
	for k, v := range(w.IPs) {
		if k == w.ID {
			continue
		}
		maxRetries := 3 // Number of retries
		retryDelay := 5*time.Second // Delay between retries
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", v)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localTCPAddr, err := net.ResolveTCPAddr("tcp", w.GetMyInternalAddrBackup(k))
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
		fmt.Printf("localAddr: %s\n", localTCPAddr.String())
		for retry := 0; retry < maxRetries; retry++ {
			d := net.Dialer{LocalAddr: localTCPAddr}
			conn, err := d.Dial("tcp", remoteTCPAddr.String())
			// conn, err := net.DialTCP("tcp", localAddr, tcpAddr)
			if err == nil {
				w.Connections[k] = conn
				fmt.Printf("Successfully established connection with the master\n")
				break
			}

			fmt.Printf("Failed to establish connection to %s - %v, retrying...\n", v, err)
			time.Sleep(retryDelay)
		}
	}
}

func (w *Worker) GetMyInternalAddrBackup(k int) string {
	addr := "127.0.0.1:3030"
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) == w.ID {
			addrList := server["internalBackup"].(map[string]interface{})
			addr = server["ip"].(string) + addrList[strconv.Itoa(k)].(string)
		}
	}
	return addr
}

func (w *Worker) GetMyExternalAddr(k int) string {
	addr := "127.0.0.1:3030"
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) == w.ID {
			addrList := server["externalPort"].(map[string]interface{})
			addr = server["ip"].(string) + addrList[strconv.Itoa(k)].(string)
		}
	}
	return addr
}

func (w *Worker) GetIncomingInternBackupID(incoming string) int {
	id := 0
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		me := int(server["id"].(float64))
		if me == w.ID {
			continue
		}
		addrList := server["internalBackup"].(map[string]interface{})
		addr := addrList[strconv.Itoa(w.ID)].(string)
		addr = "127.0.0.1" + addr
		if addr == incoming {
			id = me
			break
		}
	}
	return id
}

func (w *Worker) GetIncomingExternBackupID(remoteAddress string) int {
	id := 0
	fmt.Printf("RemoteAddress: %s\n", remoteAddress)
	masterData, _ := w.serverData["Master"].([]interface{})
	for _, worker := range masterData {
		server, _ := worker.(map[string]interface{})
		addrList := server["externalBackup"].(map[string]interface{})
		addr := server["ip"].(string) + addrList[strconv.Itoa(w.ID)].(string)
		fmt.Printf("addr: %s\n", addr)
		if addr == remoteAddress {
			id = int(server["id"].(float64))
			break
		}
	}
	return id
}

func (w *Worker) ListenTCPSocket() {
	listenAddr := w.IPs[w.ID]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
	    fmt.Printf("Worker %d Error creating listener: %v\n", w.ID, err)
	    return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Worker %d Error accepting connection: %v\n", w.ID, err)
			continue
		}
		// maintain a map so that we can find the conn according to workerID
		remoteAddress := conn.RemoteAddr().String()
		workerID := w.GetIncomingInternBackupID(remoteAddress)
		fmt.Printf("Incoming backup workerId %d\n", workerID)
		
		w.mutex.Lock()
		w.Connections[workerID] = conn
		w.numberOfWorkers = len(w.Connections)
		w.mutex.Unlock()
	}
}


func (w *Worker) ListenExternTCPSocket() {
	listenAddr := w.IPs[w.ID]
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) == w.ID {
			listenAddr = server["listenExtern"].(string)
			break
		}
	}
	
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
	    fmt.Printf("Worker %d Error creating listener: %v\n", w.ID, err)
	    return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Worker %d Error accepting connection: %v\n", w.ID, err)
			continue
		}
		// maintain a map so that we can find the conn according to workerID
		remoteAddress := conn.RemoteAddr().String()
		masterID := w.GetIncomingExternBackupID(remoteAddress)
		fmt.Printf("\n\nIncoming backup workerId %d\n\n", masterID)
		w.mutex.Lock()
		w.MasterConnection[masterID] = conn
		go w.ReceiveFromMaster(masterID)
		w.mutex.Unlock()
	}
}

//establish connections with other workers
//Worker needs to listen on IP:9999
func (w *Worker) StartListener() {
	//if this is the worker with highest ID, won't start listening
	if w.ID == w.numberOfWorkers{
		return
	}
	listenAddr := w.IPs[w.ID]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
	    fmt.Printf("Worker %d Error creating listener: %v\n", w.ID, err)
	    return
	}
	defer listener.Close()
	count := 0
	fmt.Printf("Worker %d listening on port %s\n", w.ID, listenAddr)
	for {
		conn, err := listener.Accept()
		count += 1
		if err != nil {
			fmt.Printf("Worker %d Error accepting connection: %v\n", w.ID, err)
			continue
		}
		// maintain a map so that we can find the conn according to workerID
		remoteAddress := conn.RemoteAddr().String()
		// ipAndPort := strings.Split(remoteAddress, ":")
		// ip := ipAndPort[0]
		workerID := w.GetIncomingInternID(remoteAddress)
		w.mutex.Lock()
		w.Connections[workerID] = conn
		w.mutex.Unlock()
		if count == w.numberOfWorkers-w.ID{ //If all workers with higher ID has established connection with current worker, return.
			return
		}
	}
}

func (w *Worker) GetIncomingInternID(remote string) int {
	id := 0
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) != w.ID {
			addrList := server["internalPortOutgoing"].(map[string]interface{})
			addr := server["ip"].(string) + addrList[strconv.Itoa(w.ID)].(string)
			if addr == remote {
				id = int(server["id"].(float64))
			}
		}
	}
	return id
}
/*workers have higher ID will establish connection with workers with lower ID
let's say worker 1,2,3. 
	a. 1 will start listening on port 9999
	b. 2 will start listening on port 9999 and try to establish connection with worker 1
	c. 3 will start listening on port 9999 and try to establish connection with worker 1, 2

*/

func (w *Worker)GetMyInternalOutgoingPort(id int) string{
	addr := "127.0.0.1:3030"
	workerData, _ := w.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		if int(server["id"].(float64)) == w.ID {
			addrList := server["internalPortOutgoing"].(map[string]interface{})
			addr = server["ip"].(string) + addrList[strconv.Itoa(id)].(string)
		}
	}
	return addr
}

func (w *Worker)ConnectToWorkerssWithLowerID(){
	//if this is the worker with lowest ID 1, just return.c
	if w.ID == 1{
		return
	}
	maxRetries := 5 // Number of retries
    retryDelay := 2*time.Second // Delay between retries
	time.Sleep(2*time.Second) //wait a few seconds before trying to establish connection, ensure that all workers have start listening
    count := 0
	for id := 1; id < w.ID; id++ {
        addr := w.IPs[id]
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localTCPAddr, err := net.ResolveTCPAddr("tcp", w.GetMyInternalOutgoingPort(id))
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
        for retry := 0; retry < maxRetries; retry++ {
			d := net.Dialer{LocalAddr: localTCPAddr}
			conn, err := d.Dial("tcp", remoteTCPAddr.String())
            if err == nil {
				w.mutex.Lock()
				count += 1
				w.Connections[id] = conn
				w.mutex.Unlock()
				fmt.Printf("Successfully established connection with workers with lower ID\n")
				if count == w.ID-1{
					return
				}else{
					break
				}
            }
            fmt.Printf("Failed to establish connection to %d, retrying...\n%v\n", id, err)
            time.Sleep(retryDelay)
        }
    }
}

//receive graph partition from master and will terminate once receive terminate message
func (w *Worker) ReceiveGraphPartition() {
	fmt.Println("Start receving graph partition from the master")
	fmt.Printf("Current Master: %d\n", w.currentMaster)
	reader := bufio.NewReader(w.MasterConnection[w.currentMaster])
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
			if VertexID == w.sourceVertex{
				fmt.Printf("Source vertex is on this worker, enqueue the initial kickoff message\n")
				msg := Message{
					From:  0,
					To:    VertexID,
					Value: 0.0,
					Type:  5,
				}
				w.EnqueueMessage(msg)
			}
			//hold the vertices info under worker.Vertices
			w.mutex.Lock()
			w.Vertices[VertexID] = v
			w.mutex.Unlock()
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

func (w *Worker) GetVerticesValues() map[int]float64 {
	valueMap := make(map[int]float64)
	for _, vertex := range(w.Vertices){
		if math.IsInf(vertex.Value, 1) {
			valueMap[vertex.id] = math.MaxFloat64
		} else {
			valueMap[vertex.id] = vertex.Value
		}
	}
	return valueMap
}

//receive other messages from master like startsuperstep, startexchange
func (w *Worker) ReceiveFromMaster(masterId int){
	var terminateChan chan struct{}
	if !w.isRecovered {
		go w.SendHeartBeat()
	}
	for {
		fmt.Printf("masterId: %d, MasterConn: %v\n", masterId, w.MasterConnection[masterId])
		reader := bufio.NewReader(w.MasterConnection[masterId])
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
			w.superstep +=1
			w.ProceedSuperstep()
			fmt.Printf("my vertices: %v\n", w.Vertices)
			allVerticesValue := w.GetVerticesValues()
			// After proceedsuperstep, inform the Master, mater ID field, computeFinished, type 1
			FinishedSuperstepMessage := NewMessage(w.ID, -999, allVerticesValue, 1)
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
		
		case 9://exit
			fmt.Println("Receive exit signal")
			for _, vertex := range(w.Vertices){
				fmt.Printf("Final result, vertex %d hold the value of %f\n",vertex.id,vertex.Value)
			}
			fmt.Println("Exiting...")
			w.LogExit()
			os.Exit(0)
		
		case 11:
			fmt.Printf("Receive restart signal from %d\n", message.From)
			w.currentMaster = message.From
			if !w.isRecovered {
				w.Vertices = make(map[int]*Vertex)
			}
			w.MessageQueue = []Message{}
			w.superstep = 0
			failed := message.Value.([]interface{})
			for _, nodes := range failed {
				nodeID := int(nodes.(float64))
				// w.Connections[nodeID].Close()
				delete(w.Connections, nodeID)
			}
			w.aliveNodes = []int{}
			signal := true
			allWorker := len(w.serverData["Worker"].([]interface{}))
			for i:=1; i<=allWorker; i++ {
				for _, n := range failed {
					failedM := int(n.(float64))
					fmt.Printf("now %d\n", failedM)
					if i == failedM {
						fmt.Printf("equal %d\n", i)
						signal = false
						break
					}
				}
				if signal {
					w.aliveNodes = append(w.aliveNodes, i)
				} else {
					signal = true
				}
			}
			w.numberOfWorkers = len(w.aliveNodes)
			if !w.isRecovered {
				w.ReceiveGraphPartition()
			}
			w.isRecovered = false
			fmt.Printf("alive nodes: %v\n", w.aliveNodes)

		case 12:
			states := message.Value.(map[string]interface{})
			fmt.Printf("Receive restart state %v, currentVertices: %v\n", states, w.Vertices)
			for k, v := range states{
				key, _ := strconv.Atoi(k)
				fmt.Printf("key: %d\n", key)
				if _, exists := w.Vertices[key]; exists {
					w.Vertices[key].Value = v.(float64)
					if v.(float64) < math.Inf(1) {
						w.Vertices[key].SendMessageToWorker()
					}
				}
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
	fmt.Printf("Worker %d has sent the message %v to master %d\n", message.From, message, w.currentMaster)
    
	_, err = w.MasterConnection[w.currentMaster].Write(data)
	if err != nil {
		fmt.Printf("failed to send message to master: %v\n", err)
	}
}

//send to other worker
func (w *Worker) SendMessageToWorker(workerID int, message Message) {
	// get the conn through workerID
	conn := w.Connections[workerID]
	fmt.Printf("connections: %v, conn: %d, %v\n", w.Connections, workerID, conn)
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
	fmt.Printf("vertices: %v\n", w.Vertices)
	var wg sync.WaitGroup
	for _, vertex := range w.Vertices {
		wg.Add(1)
		go func(v *Vertex) {
			defer wg.Done()
			fmt.Printf("vertice state: %v\n", v.state)
			if v.state == ACTIVE || w.superstep == 2 {
				v.Compute()
				fmt.Printf("Calculate node %d, the value is %f\n", v.id, v.Value)
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
			v, exists := w.Vertices[msg.To]
			if exists {
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
				fmt.Printf("To: %d, now have %d nodes %v\n", To, w.numberOfWorkers, w.aliveNodes)
				workerID := w.aliveNodes[w.numberOfWorkers-To%w.numberOfWorkers-1]
				fmt.Printf("Exchange: %d, %v\n", workerID, m)
				if workerID == w.ID {
					fmt.Println("Same machine, won't use tcp for exchanging messages.")
					w.EnqueueMessage(*m)
				}else{
					w.SendMessageToWorker(workerID, *m)
				}
				defer wg.Done()
			}(msg)
		default:
			wg.Wait()
			fmt.Printf("superstep: %d\n", w.superstep)
			if !sendMessage && w.superstep != 1{
				// At the very beginning, no message in the channel, send message to master. Type 3.
				EmptyMessage := NewMessage(w.ID, -999, nil, 3)
				w.SendMessageToMaster(*EmptyMessage)
				fmt.Printf("Worker %d has no outgoing messages to send.\n",w.ID)
				return
			}else{
				//send type2 Worker has finished sending out all outgoing messages.
				FinishedSendingMessage := NewMessage(w.ID, -999, nil, 2)
				w.SendMessageToMaster(*FinishedSendingMessage)
				fmt.Printf("Worker %d has sent out all outgoing messages.\n",w.ID)
				return
			}
		}
	}
}

func (w *Worker) SendHeartBeat() {
	HeartBeatMessage := NewMessage(w.ID, -999, nil, 10)
	for {
		fmt.Printf("Preparing to send the heartbeat\n")
		go w.SendMessageToMaster(*HeartBeatMessage)
		time.Sleep(2 * time.Second)
	}
}

//
func NewWorker(ID int) *Worker{
	return &Worker{
		ID:             ID,
		Vertices:       make(map[int]*Vertex),
		MessageQueue:   []Message{},
		workerChan:     make(chan *Message,100),
		IPs:            make(map[int]string),
		reverse_IPs:    make(map[string]int),
		Connections:    make(map[int]net.Conn),
		MasterConnection: make(map[int]net.Conn),
		MasterAddr: make(map[int]string),
		mutex:          sync.Mutex{},
		numberOfWorkers:  1, // set number of workers,
		sourceVertex: 1,
		superstep: 0,
		serverData: make(map[string]interface{}),
		aliveNodes: []int{},
		currentMaster: 3,
		isRecovered: false,
	}
}

func (w *Worker) Log() bool{
	var lastLine string
	isRecovered := true
	logFile := "worker" + strconv.Itoa(w.ID) + ".txt"
	file, err := os.Open(logFile)
	if err != nil {
        fmt.Println("Error opening file:", err)
        return isRecovered
    }
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
        lastLine = scanner.Text()
    }
	fmt.Printf("%s\n", lastLine)
	if lastLine == "" || lastLine == "Exit" {
		isRecovered = false
	} else {
		fmt.Println("It is detected that the node is recovered from crash...")
	}

	file, err = os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file for writing:", err)
		return isRecovered
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
    _, err = writer.WriteString("Start\n")
    if err != nil {
        fmt.Println("Error writing to file:", err)
        return isRecovered
    }

    err = writer.Flush()
    if err != nil {
        fmt.Println("Error flushing writer:", err)
    }

	w.isRecovered = isRecovered
	return isRecovered
}

func (w *Worker) LogExit() {
	logFile := "worker" + strconv.Itoa(w.ID) + ".txt"
	
	file, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file for writing:", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
    _, err = writer.WriteString("Exit\n")
    if err != nil {
        fmt.Println("Error writing to file:", err)
    }

    err = writer.Flush()
    if err != nil {
        fmt.Println("Error flushing writer:", err)
    }
}

func (w *Worker) PrepareAfterRecovery() {
	var wg sync.WaitGroup
	//Establish connection with master
	//while workers are trying to e currently will keep running and not killedstablish connection between each other, we can start receving partitions from the master
	wg.Add(1)
	go func(){
		w.ReceiveGraphPartition()
		wg.Done()
	}()	
	wg.Add(1)
	go func(){
		w.ConnectToWorkers()
		wg.Done()
	}()
	// only proceed after receving all partitions and establish all connections
	wg.Wait()
}

// call all necessary functions for the worker
func (w *Worker) Run(){
	w.GetServerData()
	isRecovered := w.Log()
	
	if isRecovered {
		w.EstablishMasterConnectionBackup() 
		go w.SendHeartBeat()
		w.PrepareAfterRecovery()
	} else {
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
	}

	fmt.Println("Start listening instructions from Master")
	//continue receiving instructions from master.
	go w.ListenTCPSocket()
	go w.ListenExternTCPSocket()
	for k, _ := range w.MasterConnection {
		go w.ReceiveFromMaster(k)
	}
}

