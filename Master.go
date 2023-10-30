package main

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	highestID   int // the highest id of all workers
	mapLock     sync.Mutex
	workersMap  map[int]*WorkerConnection // map id to worker connection
	inCh        chan Message
	finishCount int
}

func (m *Master) RegisterWorker(conn net.Conn) *WorkerConnection {
	m.mapLock.Lock()
	m.highestID++
	wc := &WorkerConnection{
		ID:     m.highestID,
		Addr:   conn.RemoteAddr().String(),
		C:      make(chan any, 100),
		conn:   conn,
		master: m,
	}
	m.workersMap[wc.ID] = wc
	m.mapLock.Unlock()
	return wc
}

func NewMaster() *Master {
	return &Master{
		highestID:   0, // worker ID allocation start from 0
		mapLock:     sync.Mutex{},
		workersMap:  make(map[int]*WorkerConnection),
		inCh:        make(chan Message, 500),
		finishCount: 0,
	}
}

func (m *Master) ListenWorkerConnections() {
	for {
		inMessage := <-m.inCh
		fmt.Println(inMessage)
		m.ProcessMessage(inMessage)
	}
}

func (m *Master) Start() {
	// Create a TCP listener on port 8080
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()
	fmt.Println("Listening on port 8080")
	go m.ListenWorkerConnections()
	// Accept incoming connections and handle them in a separate goroutine
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go m.HandleConnection(conn)
	}

	// TODO: when to start partition
}

// HandleConnection handles each incoming connection and prints the client's address
func (m *Master) HandleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("New connection from", conn.RemoteAddr())
	wc := m.RegisterWorker(conn)
	go wc.Run()

	// TODO: handle connection exceptions
}

func (m *Master) GraphDistribution() {
	nodes := ParseInput("SampleInput.json")
	parts := Partition(nodes, 3)
	fmt.Println(parts)
	m.mapLock.Lock()
	receiverID := 0
	for _, part := range parts {
		for _, node := range part {
			m.workersMap[receiverID].C <- node
		}
	}
	m.mapLock.Unlock()
	m.InstructNextStep()
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

func (m *Master) ProcessMessage(message Message) {
	switch message.Type {
	case COMPUTE_FINISH:
		m.finishCount++
		if m.finishCount == m.highestID {
			m.InstructExchange()
			m.finishCount = 0
		}
	case SEND_FINISH:
		m.finishCount++
		if m.finishCount == m.highestID {
			m.InstructNextStep()
			m.finishCount = 0
		}

	}
}