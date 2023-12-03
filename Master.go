package main

import (
	"fmt"
	"net"
	"sync"
	"time"
	"strconv"
	"encoding/json"
	"io/ioutil"
	"bufio"
	"sort"
	"os"
)

var start time.Time = time.Now()

type Master struct {
	id int
	isPrimary int
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
	IPs map[int]string 
	reverse_IPs map[string]int 
	MasterConnection map[int]net.Conn
	masterHeartBeatChan chan Message
	electionResChan chan Message
	planningToStart bool
	isRecovered bool
}

func (m *Master) RegisterWorker(conn net.Conn) *WorkerConnection {
	m.mapLock.Lock()
	workerId := m.GetWorkerId(conn.RemoteAddr().String())
	fmt.Printf("workerId: %d\n", workerId)

	signal := false
	for _, val := range(m.aliveWorkers) {
		if val == workerId{
			signal = true
			break
		}	
	}
	if !signal {
		m.aliveWorkers = append(m.aliveWorkers, workerId)
		sort.Ints(m.aliveWorkers)
	}
	wc := &WorkerConnection{
		ID:     workerId,
		Addr:   conn.RemoteAddr().String(),
		C:      make(chan any, 100), //is it the channel of Message struct??
		conn:   conn,
		master: m,
	}
	m.workersMap[workerId] = wc
	m.mapLock.Unlock()
	return wc
}

func NewMaster(id int) *Master {	
	return &Master{
		id: id,
		isPrimary: 5,
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
		IPs: make(map[int]string), 
	    reverse_IPs: make(map[string]int),
		masterHeartBeatChan: make(chan Message),
		MasterConnection: make(map[int]net.Conn),
		electionResChan: make(chan Message),
		planningToStart: false,
		isRecovered: false,
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
	masterData, _ := m.serverData["Master"].([]interface{})
	m.numberOfWorker = len(workerData)

	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		addr := server["ip"].(string) + server["internalPort"].(string)
		masterId := int(server["id"].(float64))
		m.IPs[masterId] = addr
		m.reverse_IPs[addr] = masterId
	}
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

func (m *Master) GetMyInternalPort() string{
	portNum := ":3030"
	masterData, _ := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			portNum = server["internalPort"].(string)
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
		addrList := server["externalPort"].(map[string]interface{})
		addr := server["ip"].(string) + addrList[strconv.Itoa(m.id)].(string)
		fmt.Printf("addr: %s\n", addr)
		if addr == remoteAddress {
			id = int(server["id"].(float64))
		}
	}
	// deleting
	if id == 0 {
		for _, worker := range workerData {
			server, _ := worker.(map[string]interface{})
			addrList := server["externalBackup"].(map[string]interface{})
			addr := "127.0.0.1" + addrList[strconv.Itoa(m.id)].(string)
			if addr == remoteAddress {
				id = int(server["id"].(float64))
			}
		}
	}
	return id
}

func (m *Master) GetMasterID(remoteAddress string) int{
	id := 0
	fmt.Printf("RemoteAddress: %s\n", remoteAddress)
	masterData, _ := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			continue
		}
		addrList := server["internalBackup"].(map[string]interface{})
		addr := server["ip"].(string) + addrList[strconv.Itoa(m.id)].(string)
		fmt.Printf("addr: %s\n", addr)
		if addr == remoteAddress {
			id = int(server["id"].(float64))
			break
		}
	}
	return id
}

func (m *Master) LogExit() {
	logFile := "master" + strconv.Itoa(m.id) + ".txt"
	
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

func (m *Master) SendMessageToMaster(masterID int, message Message) {
	// get the conn through workerID
	conn := m.MasterConnection[masterID]
	if conn == nil {
		return
	}
    data, err := json.Marshal(message)
	if err != nil {
        fmt.Printf("failed to marshal the message: %v\n", err)
    }	
    // seperate each data by \n
    data = append(data, '\n')
    _, err = conn.Write(data)
    if err != nil {
        fmt.Printf("failed to send message to other master: %v\n", err)
    }
}

func (m *Master) handleIncomingMessagesFromConn(k int) {
	fmt.Printf("Begin to listen the sync messages from %d.\n", k)
    for {
		// if (m.isPrimary == m.id){
		// 	continue
		// }
		fmt.Printf("my master connections: %v.\n", m.MasterConnection)
		if m.MasterConnection[k] == nil {
			continue
		}
		reader := bufio.NewReader(m.MasterConnection[k])
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
		fmt.Printf("Received messages from master %d with type %d.\n", message.From, message.Type)
		if message.Type == 13 {
			m.UpdateMasterState(message)
		} else if message.Type == 14 {
			go func() {
				m.masterHeartBeatChan <- message
				fmt.Printf("Length: %d\n", len(m.masterHeartBeatChan))
			}()
		} else if message.Type == 15 {
			fmt.Printf("Receive election start from %d\n", message.From)
			if m.id > message.From {
				fmt.Printf("Send a rejection to %d\n", message.From)
				msg := NewMessage(m.id, -999, nil, 17)
				go m.SendMessageToMaster(message.From, *msg)
			}
		} else if message.Type == 16 {
			fmt.Printf("Receive election annoucement from %d\n", message.From)
			m.isPrimary = message.From
		} else if message.Type == 17{
			go func() {
				m.electionResChan <- message
				fmt.Printf("Receive election rejection from %d\n", message.From)
			}()
		} else if message.Type == EXIT {
			passed := time.Since(start).Seconds()
			fmt.Printf("Performace: Time used - %v\n", passed)
			m.LogExit()
			os.Exit(0)
		}
    }
}

func (m *Master) StartElection() {
	electionMessage := NewMessage(m.id, -999, nil, 15)
	isBiggest := true
	for k, _ := range(m.IPs) {
		if k > m.id {
			isBiggest = false
			fmt.Printf("Preparing to send election from %d to %d\n", m.id, k)
			go m.SendMessageToMaster(k, *electionMessage)
		}	
	}
	announcementMessage := NewMessage(m.id, -999, nil, 16)
	if isBiggest {
		m.isPrimary = m.id
		for k, _ := range(m.IPs) {
			if k != m.id {
				fmt.Printf("Preparing to announce election from %d to %d\n", m.id, k)
				go m.SendMessageToMaster(k, *announcementMessage)
			}	
		}
		time.Sleep(1*time.Second)
		go m.Restart()
	} else {
		select {
        case response := <- m.electionResChan:
			fmt.Printf("Replica %d: Failed the election due to %d.\n", m.id, response.From)	
		case <- time.After(5 * time.Second):
            m.isPrimary = m.id
			fmt.Printf("Replica %d: Won the election.\n", m.id)
			for k, _ := range(m.IPs) {
				if k != m.id {
					fmt.Printf("Preparing to announce election from %d to %d\n", m.id, k)
					go m.SendMessageToMaster(k, *announcementMessage)
				}	
			}
			time.Sleep(1*time.Second)
			go m.Restart()
        }
	}
}

func (m *Master) CheckMasterHeartBeat() {
	for m.id != m.isPrimary {
		select {
		case msg := <-m.masterHeartBeatChan:
			fmt.Println("Received heart beat from the primary master:", msg)
		case <-time.After(5 * time.Second):
			fmt.Println("Detect the primary master is dead...")
			if m.id != m.isPrimary {
				go m.StartElection()
			}
		}		
	}
}

func (m *Master) UpdateMasterState(message Message) {
	msg := message.Value.(map[string]interface{})
	for k, v := range msg{
		key, _ := strconv.Atoi(k)
		m.verticesValue[key] = v.(float64)
	}
	fmt.Printf("Sync verticesValue to %v\n", m.verticesValue)
}

func (m *Master) ListenTCPSocket() {
	port := m.GetMyExternalPort()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Again I am listening on port %s\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("A node is trying to recover %v\n", err)
			continue
		}
		m.HandleConnection(conn)
		fmt.Printf("\n\nA node has recovered.\n\n")
		if m.id == m.isPrimary {
			m.Restart()
		}
	}
	defer listener.Close()
}

func (m *Master) ListenInternTCPSocket() {
	port := m.GetMyInternalPort()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Again I am listening on port %s\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("A node is trying to recover %v\n", err)
			continue
		}
		masterId := m.GetMasterID(conn.RemoteAddr().String())
		m.MasterConnection[masterId] = conn
		go m.handleIncomingMessagesFromConn(masterId)
		fmt.Printf("\n\nA master has recovered.\n\n")
	}
	defer listener.Close()
}

func (m *Master) GetLocalAddressInternal(id int) string {
	localAddr := "127.0.0.1:3000"
	masterData := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			init := server["internalInit"].(map[string]interface{})
			localAddr = server["ip"].(string) + init[strconv.Itoa(id)].(string)
			fmt.Printf("init: %v, port: %s, localAddr: %v\n", init, init[strconv.Itoa(id)].(string), localAddr)
		}
	}
	return localAddr
}

func (m *Master) GetMasterIP(id int) string {
	ip := "127.0.0.1"
	masterData := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == id {
			ip = server["ip"].(string)
		}
	}
	return ip
}

func (m *Master) GetIncomingConnAddress(remoteAddress string) int {
	id := 1
	masterData := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			init := server["internalComing"].(map[string]interface{})
			for k, v := range(init){
				key, _ := strconv.Atoi(k)
				if remoteAddress == m.GetMasterIP(key)+v.(string){
					id = key
				}
			}
		}
	}
	return id
}

func (m *Master)ConnectToMastersWithLowerID(){
	//if this is the worker with lowest ID 1, just return.c
	if m.id == 1{
		return
	}
	maxRetries := 5 // Number of retries
    retryDelay := 2*time.Second // Delay between retries
	time.Sleep(2*time.Second) //wait a few seconds before trying to establish connection, ensure that all workers have start listening
    count := 0
	for id := 1; id < m.id; id++ {
        addr := m.IPs[id]
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localAddr := m.GetLocalAddressInternal(id)
		localTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
        for retry := 0; retry < maxRetries; retry++ {
			d := net.Dialer{LocalAddr: localTCPAddr}
			conn, err := d.Dial("tcp", remoteTCPAddr.String())
            if err == nil {
				m.mapLock.Lock()
				count += 1
				m.MasterConnection[id] = conn
				m.mapLock.Unlock()
				fmt.Printf("Successfully established connection with masters with lower ID, %s -> %s\n", localAddr, addr)
				if count == m.id-1{
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

func (m *Master) StartMasterListener() {
	//if this is the worker with highest ID, won't start listening
	if m.id == len(m.IPs){
		return
	}
	listenAddr := m.IPs[m.id]
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
	    fmt.Printf("Master %d Error creating listener: %v\n", m.id, err)
	    return
	}
	defer listener.Close()
	count := 0
	fmt.Printf("Master %d listening on port %s\n", m.id, listenAddr)
	for {
		conn, err := listener.Accept()
		count += 1
		if err != nil {
			fmt.Printf("Master %d Error accepting connection: %v\n", m.id, err)
			continue
		}
		// maintain a map so that we can find the conn according to workerID
		remoteAddress := conn.RemoteAddr().String()
		// ipAndPort := strings.Split(remoteAddress, ":")
		// ip := ipAndPort[0]
		masterID := m.GetIncomingConnAddress(remoteAddress)
		fmt.Printf("507: masterID - %d\n", masterID)
		m.mapLock.Lock()
		m.MasterConnection[masterID] = conn
		m.mapLock.Unlock()
		if count == len(m.IPs)-m.id{ //If all workers with higher ID has established connection with current worker, return.
			fmt.Printf("...........................%v, %d", m.IPs, m.id)
			return
		}
	}
}

func (m *Master) StartWorkerListener() {
	// Create a TCP listener on port 8080
	port := m.GetMyExternalPort()
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Listening on port %s\n", port)
	// Accept incoming connections and handle them in a separate goroutine
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		m.connectedNum++
		m.HandleConnection(conn)
		if m.connectedNum == m.numberOfWorker {
			break
		}
	}
	listener.Close()
	return
}

func (m *Master) SendHeartBeat() {
	HeartBeatMessage := NewMessage(m.id, -999, nil, 14)
	fmt.Printf("IPS: %v\n", m.IPs)
	for {
		if (m.isPrimary == m.id) {
			for k, _ := range m.IPs{
				if k != m.isPrimary {
					fmt.Printf("Preparing send hb to %d\n", k)
					go m.SendMessageToMaster(k, *HeartBeatMessage)
				}
			}
		    time.Sleep(4 * time.Second)
		}
	}
}

func (m *Master) GetLocalInternPortBackup(k int) string{
	addr := "127.0.0.1:3030"
	masterData, _ := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			addrList := server["internalBackup"].(map[string]interface{})
			addr = server["ip"].(string) + addrList[strconv.Itoa(k)].(string)
		}
	}
	return addr
}

func (m *Master) ConnectToMastersAfterRecovery(){
	maxRetries := 5 // Number of retries
    retryDelay := 2*time.Second // Delay between retries
	time.Sleep(2*time.Second) //wait a few seconds before trying to establish connection, ensure that all workers have start listening
    count := 0
	for k, v := range(m.IPs) {
		if k == m.id {
			continue
		}
        addr := v
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localAddr := m.GetLocalInternPortBackup(k)
		localTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
        for retry := 0; retry < maxRetries; retry++ {
			d := net.Dialer{LocalAddr: localTCPAddr}
			conn, err := d.Dial("tcp", remoteTCPAddr.String())
            if err == nil {
				m.mapLock.Lock()
				count += 1
				m.MasterConnection[k] = conn
				go m.handleIncomingMessagesFromConn(k)
				m.mapLock.Unlock()
				fmt.Printf("Successfully established connection with masters, %s -> %s\n", localAddr, addr)
				if count == len(m.IPs)-1{
					return
				}else{
					break
				}
            }
            fmt.Printf("Failed to establish connection to %d, retrying...\n%v\n", k, err)
            time.Sleep(retryDelay)
        }
    }
}

func (m *Master) GetLocalExternPortBackup(k int) string{
	localAddr := "127.0.0.1:3000"
	masterData, _ := m.serverData["Master"].([]interface{})
	for _, master := range masterData {
		server, _ := master.(map[string]interface{})
		if int(server["id"].(float64)) == m.id {
			addrList := server["externalBackup"].(map[string]interface{})
			localAddr = server["ip"].(string) + addrList[strconv.Itoa(k)].(string)
		}
	}
	return localAddr
}

func (m *Master) ConnectToWorkersAfterRecovery(){
	workerPorts := make(map[int]string)
	workerData := m.serverData["Worker"].([]interface{})
	for _, worker := range workerData {
		server, _ := worker.(map[string]interface{})
		workerPorts[int(server["id"].(float64))] = "127.0.0.1" + server["listenExtern"].(string)
	}

    count := 0
	fmt.Printf("worker ports: %v\n", workerPorts)
	for k, v := range(workerPorts) {
        addr := v
		remoteTCPAddr, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("failed to resolve address: %v\n", err)
		}
		localAddr := m.GetLocalExternPortBackup(k)
		localTCPAddr, err := net.ResolveTCPAddr("tcp", localAddr)
		if err != nil {
			fmt.Println("Error resolving local address:", err)
			return
		}
		d := net.Dialer{LocalAddr: localTCPAddr}
		conn, err := d.Dial("tcp", remoteTCPAddr.String())
		if err == nil {
			m.mapLock.Lock()
			count += 1
			wc := &WorkerConnection{
				ID:     k,
				Addr:   conn.RemoteAddr().String(),
				C:      make(chan any, 100), //is it the channel of Message struct??
				conn:   conn,
				master: m,
			}
			m.workersMap[k] = wc
			m.aliveWorkers = append(m.aliveWorkers, k)
			m.mapLock.Unlock()
			go wc.Run()
			fmt.Printf("Successfully established connection with worker %d, %s -> %s\n", k, localAddr, addr)
		} else {
            fmt.Printf("Failed to establish connection to %d due to %v\n", k, err)
		}   
    }
}

func (m *Master) Start() {
	isRecovered := m.Log()
	m.GetServerData()

	if isRecovered {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(){
			m.ConnectToWorkersAfterRecovery()
			wg.Done()
		}()
		wg.Add(1)
		go func(){
			m.ConnectToMastersAfterRecovery()
			wg.Done()
		}()
		wg.Wait()
		m.StartElection()
	} else {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(){
			m.StartMasterListener()
			wg.Done()
		}()
		wg.Add(1)
		go func(){
			m.StartWorkerListener()
			wg.Done()
		}()
		wg.Add(1)
		go func(){
			m.ConnectToMastersWithLowerID()
			wg.Done()
		}()
		wg.Wait()
	}

	go m.ListenWorkerConnections()
	go m.ListenTCPSocket()
	go m.ListenInternTCPSocket()
	go m.CheckWorkerHeartBeat()
	go m.CheckMasterHeartBeat()
	go m.SendHeartBeat()
	for k, _ := range m.IPs {
		if k != m.id {
			fmt.Printf("716: %d\n", k)
			go m.handleIncomingMessagesFromConn(k)
		}
	}

	if m.isPrimary == m.id && !m.isRecovered{
		m.GraphDistribution()
		fmt.Println("partition finished, wait for a few seconds")
		time.Sleep(1*time.Second)
		fmt.Println("All partition has been sent")
		m.InformPartitionFinish()
		time.Sleep(5*time.Second)
		fmt.Println("Let's proceed the superstep")
		m.InstructNextStep()
	}
}

func (m *Master) Restart() {
	m.numberOfWorker = len(m.aliveWorkers)
	m.finishCount = 0
	m.InformRestart()
	time.Sleep(2*time.Second)
	m.planningToStart = false
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
	sort.Ints(m.aliveWorkers)
	nodes := ParseInput("Test/SampleNodes50.json")
	parts := Partition(nodes, m.numberOfWorker)
	fmt.Println(parts)
	m.mapLock.Lock()
	//TODO: disttibute the graph. Now only work for one worker, we need to know all workers.
	for id, part := range parts {
		receiver := m.aliveWorkers[id]
		fmt.Printf("Send %v to receiver %d: %d\n", part, id, receiver)
		for _, node := range part {
			m.workersMap[receiver].C <- node
		}
	}
	m.mapLock.Unlock()
}

func (m *Master) UpdateState() {
	states := make(map[int](map[int]float64), m.numberOfWorker)
	fmt.Printf("Updating states - vertices values: %v, number of workers: %d, aliveWorkers: %v\n", m.verticesValue, m.numberOfWorker, m.aliveWorkers)
	for k, v := range m.verticesValue {
		server_id := m.aliveWorkers[k%m.numberOfWorker]
		if _, ok := states[server_id]; !ok {
			states[server_id] = make(map[int]float64)
		}
		states[server_id][k] = v
	}
	fmt.Printf("states: %v\n", states)
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
	for k, _ := range m.IPs {
		if k!=m.id{
			updateMsg := Message {
				From: m.id,
				To: k,
				Value: m.verticesValue,
				Type: MASTER_SYNC,
			}
			go m.SendMessageToMaster(k, updateMsg)
		}
	}
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
		if (m.id != m.isPrimary) {
			continue
		}			
		time.Sleep(5 * time.Second)
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
				sort.Ints(m.aliveWorkers)
			}
		}
		fmt.Printf("Dead: %v\nAlive: %v\n", dead, m.aliveWorkers)
		if dead > 0 {
			m.planningToStart = true
			go m.Restart()
		}	
	}
}

func (m *Master) InformRestart(){
	failed := m.GetFailedNodesID()
	fmt.Printf("Failed: %v, workersMap: %v\n", failed, m.workersMap)
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  m.id,
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
		From:  m.id,
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
		From:  m.id,
		To:    i,
		Value: nil,
		Type:  EXIT,
	  }
	}
	for i, _ := range m.MasterConnection {
		msg := Message{
			From:  m.id,
			To:    i,
			Value: nil,
			Type:  EXIT,
		}
		m.SendMessageToMaster(i, msg)
	}
	m.mapLock.Unlock()
}

func (m *Master) InstructExchangeStop(){
	m.mapLock.Lock()
	for i, connection := range m.workersMap {
	  connection.C <- Message{
		From:  m.id,
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
		m.mapLock.Lock()
		m.verticesValue[intVal] = val.(float64)
		m.mapLock.Unlock()
	}
}

func (m *Master) Log() bool{
	var lastLine string
	isRecovered := true
	logFile := "master" + strconv.Itoa(m.id) + ".txt"
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

	m.isRecovered = isRecovered
	return isRecovered
}

func (m *Master) ProcessMessage(message Message) {
	sig := false
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
		} else if m.emptyCount + m.finishCount == m.numberOfWorker {
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
			fmt.Printf("Final result: %v\n", m.verticesValue)
			m.LogExit()
			passed := time.Since(start).Seconds()
			fmt.Printf("Performace: Time used - %v\n", passed)
			sig = true
			time.Sleep(2*time.Second)
		} else if m.emptyCount + m.finishCount == m.numberOfWorker {
			time.Sleep(5*time.Second)
			m.InstructExchangeStop()
			m.finishCount = 0
			m.emptyCount = 0
			time.Sleep(1*time.Second)
			m.InstructNextStep()
		}
	}
	if sig {
		os.Exit(0)
	}
}
