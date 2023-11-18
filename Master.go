package main

import (
	"fmt"
	"net"
	"sync"
	"time"
	"strconv"
	"encoding/json"
	"io/ioutil"
)

type Master struct {
	id int
	mapLock        sync.Mutex
	workersMap     map[int]*WorkerConnection // map id to worker connection
	inCh           chan Message
	finishCount    int
	numberOfWorker int
	connectedNum   int
	emptyCount int
	verticesValue map[int]float64
	workerHeartBeat chan Message
	serverData map[string]interface{}
	aliveWorkers []int
}

func (m *Master) RegisterWorker(conn net.Conn) *WorkerConnection {
	m.mapLock.Lock()
	workerId := m.GetWorkerId(conn.RemoteAddr().String())
	m.aliveWorkers = append(m.aliveWorkers, workerId)
	wc := &WorkerConnection{
		ID:     workerId,
		Addr:   conn.RemoteAddr().String(),
		C:      make(chan any, 100), //is it the channel of Message struct??
		conn:   conn,
		master: m,
	}
	m.workersMap[wc.ID] = wc
	m.mapLock.Unlock()
	return wc
}

func NewMaster(id int) *Master {
	return &Master{
		id: id,
		mapLock:        sync.Mutex{},
		workersMap:     make(map[int]*WorkerConnection),
		inCh:           make(chan Message, 500),
		finishCount:    0,
		numberOfWorker: 0,
		connectedNum:   0,
		emptyCount: 0,
		verticesValue: make(map[int]float64),
		workerHeartBeat: make(chan Message, 500),
		serverData: make(map[string]interface{}),
		aliveWorkers: []int{},
	}
}

func (m *Master) ListenWorkerConnections() {
	for {
		inMessage := <-m.inCh
		//fmt.Println(inMessage)
		go m.ProcessMessage(inMessage)
	}
}

func (m *Master) GetServerData() {
	fileContent, err := ioutil.ReadFile("ServerAddress.json")
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// Unmarshal the JSON data into the map
	err = json.Unmarshal(fileContent, &m.serverData)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return
	}
	fmt.Printf("Server Data: %v\n", m.serverData)

	workerData, _ := m.serverData["Worker"].([]interface{})
	m.numberOfWorker = len(workerData)
}

func (m *Master) GetMyExternalPort() string{
	portNum := ":3030"
	masterData, _ := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			portNum = server["externalPort"].(string)
		}
	}
	return portNum
}

func (m *Master) GetWorkerId(remoteAddress string) int{
	id := 0
	fmt.Printf("RemoteAddress: %s\n", remoteAddress)
	workerData, _ := m.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		addr := server["ip"].(string) + server["externalPort"].(string)
		fmt.Printf("addr: %s\n", addr)
		if addr == remoteAddress {
			id = int(server["id"].(float64))
		}
	}
	return id
}

func (m *Master) ListenTCPSocket(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		m.HandleConnection(conn)
		fmt.Printf("A node has recovered.\n")
		m.Restart()
	}
	defer listener.Close()
}

func (m *Master) Start() {
	m.GetServerData()
	// Create a TCP listener on port 8080
	port := m.GetMyExternalPort()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	fmt.Printf("Listening on port %s\n", port)
	go m.ListenWorkerConnections()
	// Accept incoming connections and handle them in a separate goroutine
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		m.connectedNum++
		go m.HandleConnection(conn)
		if m.connectedNum == m.numberOfWorker {
			break
		}
	}
	// time.Sleep(3*time.Second)
	go m.CheckWorkerHeartBeat()
	go m.ListenTCPSocket(listener)
	m.GraphDistribution()
	fmt.Println("partition finished, wait for a few seconds")
	time.Sleep(1*time.Second)
	fmt.Println("All partition has been sent")
	m.InformPartitionFinish()
	time.Sleep(5*time.Second)
	fmt.Println("Let's proceed the superstep")
	m.InstructNextStep()
}

func (m *Master) Restart() {
	m.numberOfWorker = len(m.aliveWorkers)
	m.finishCount = 0
	m.InformRestart()
	time.Sleep(2*time.Second)
	m.GraphDistribution()
	fmt.Println("partition redistribution finished, wait for a few seconds")
	time.Sleep(1*time.Second)
	fmt.Println("All partition has been sent")
	m.InformPartitionFinish()
	time.Sleep(4*time.Second)
	m.UpdateState()
	time.Sleep(2*time.Second)
	fmt.Println("Let's proceed the superstep")
	m.InstructNextStep()
}

// HandleConnection handles each incoming connection and prints the client's address
func (m *Master) HandleConnection(conn net.Conn) {
	//defer conn.Close() the connection is always on.
	fmt.Println("New connection from", conn.RemoteAddr())
	wc := m.RegisterWorker(conn)
	go wc.Run()

	// TODO: handle connection exceptions
}

func (m *Master) GraphDistribution() {
	nodes := ParseInput("SampleInput.json")
	parts := Partition(nodes, m.numberOfWorker)
	fmt.Println(parts)
	fmt.Println(m.workersMap)
	m.mapLock.Lock()
	//TODO: disttibute the graph. Now only work for one worker, we need to know all workers.
	for id, part := range parts {
		fmt.Println(part)
		receiver := m.aliveWorkers[m.numberOfWorker-id-1]
		for _, node := range part {
			m.workersMap[receiver].C <- node
			fmt.Printf("Send the vertice to %d - %v\n", receiver, node)
		}
	}
	m.mapLock.Unlock()
}

func (m *Master) UpdateState() {
	states := make(map[int](map[int]float64), m.numberOfWorker)
	for k, v := range m.verticesValue {
		server_id := m.aliveWorkers[m.numberOfWorker-k%m.numberOfWorker-1]
		states[server_id] = make(map[int]float64)
		states[server_id][k] = v
	}
	for k, v := range states {
		m.workersMap[k].C <- Message {
			From:  0,
			To:    k,
			Value: v,
			Type:  RESTART_STATE,
		}
	}
}

func (m *Master) InstructNextStep() {
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
		connection.C <- Message{
			From:  0,
			To:    i,
			Value: nil,
			Type:  START_NEXT,
		}
	}
	m.mapLock.Unlock()
}

func (m *Master) InstructExchange() {
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
		connection.C <- Message{
			From:  0,
			To:    i,
			Value: nil,
			Type:  EXCHANGE_START,
		}
	}
	m.mapLock.Unlock()
}

func (m *Master) CheckWorkerHeartBeat() {
	for {			
		time.Sleep(8 * time.Second)
		aliveness := make(map[int]bool)
		for i := 0; i < len(m.aliveWorkers); i++ {
			aliveness[m.aliveWorkers[i]] = false
		}
		length := len(m.workerHeartBeat)
		for i := 0; i < length; i++ {
			msg := <-m.workerHeartBeat
			aliveness[msg.From] = true
		}
		fmt.Printf("Aliveness: %v, %d\n", aliveness, length)
		m.aliveWorkers = []int{}
		dead := 0
		for k, _ := range aliveness {
			if !aliveness[k] {
				dead += 1
				delete(m.workersMap, k)
			} else {
				m.aliveWorkers = append(m.aliveWorkers, k)
			}
		}
		fmt.Printf("Dead: %v\nAlive: %v\n", dead, m.aliveWorkers)
		if dead > 0{
			go m.Restart()
			return
		}	
	}
}

func (m *Master) InformRestart(){
	failed := m.GetFailedNodesID()
	fmt.Printf("Failed: %v\n", failed)
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  0,
		To:    i,
		Value: failed,
		Type:  RESTART,
	  }
	}
}

func (m *Master) GetFailedNodesID() []int{
	allWorker := len(m.serverData["Worker"].([]interface{}))
	deadNodes := []int{}
	present := make(map[int]bool)
	for _, num := range m.aliveWorkers {
		present[num] = true
	}
	for i := 1; i <= allWorker; i++ {
		if !present[i] {
			deadNodes = append(deadNodes, i)
		}
	}
	fmt.Printf("Alive workers: %v, allWorker: %d, deadNodes: %v\n", m.aliveWorkers, allWorker, deadNodes)
	return deadNodes
}

func (m *Master) InformPartitionFinish(){
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  0,
		To:    i,
		Value: nil,
		Type:  ASSIGN_FINISHED,
	  }
	}
	m.mapLock.Unlock()
}
func (m *Master) InstructExit(){
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  0,
		To:    i,
		Value: nil,
		Type:  EXIT,
	  }
	}
	m.mapLock.Unlock()
}

func (m *Master) InstructExchangeStop(){
	fmt.Printf("Current vertices values: %v\n", m.verticesValue)
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  0,
		To:    i,
		Value: nil,
		Type:  EXCHANGE_STOP,
	  }
	}
	m.mapLock.Unlock()
}

func (m *Master) UpdateVerticesValues(values interface{}) {
	fmt.Printf("%v\n", values)
	valMap := values.(map[string]interface{})
	for id, val := range valMap {
		intVal, _ := strconv.Atoi(id)
		m.verticesValue[intVal] = val.(float64)
	}
}

func (m *Master) ProcessMessage(message Message) {
	fmt.Printf("%v\n", message)
	switch message.Type {
	case COMPUTE_FINISH:
		m.finishCount++
		m.UpdateVerticesValues(message.Value)
		fmt.Printf("Receive the compute_finish. FinishCount: %d, numberOfWorker: %d\n", m.finishCount, m.numberOfWorker)
		if m.finishCount == m.numberOfWorker {
			m.InstructExchange()
			m.finishCount = 0
		}
	case SEND_FINISH:
		m.finishCount++
		fmt.Printf("finishCount: %d\n",m.finishCount)
		if m.finishCount == m.numberOfWorker{
			time.Sleep(2*time.Second)
			m.InstructExchangeStop()
			m.finishCount = 0
			time.Sleep(2*time.Second)
			m.InstructNextStep()
			
		}
		if m.emptyCount + m.finishCount == m.numberOfWorker {
			time.Sleep(5*time.Second)
			m.InstructExchangeStop()
			m.finishCount = 0
			m.emptyCount = 0
			time.Sleep(1*time.Second)
			m.InstructNextStep()
		}

	case SEND_EMPTY:
		m.emptyCount++
		fmt.Printf("emptyCount: %d finishCount: %d\n",m.emptyCount, m.finishCount)
		if m.emptyCount == m.numberOfWorker {
			m.InstructExit()
			fmt.Println("No more exchanging messages and all vertices are IDLE, send EXIT message to all workers")
			time.Sleep(5*time.Second)
			return
		}
		if m.emptyCount + m.finishCount == m.numberOfWorker {
			time.Sleep(5*time.Second)
			m.InstructExchangeStop()
			m.finishCount = 0
			m.emptyCount = 0
			time.Sleep(1*time.Second)
			m.InstructNextStep()
		}
	}
}
