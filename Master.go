package main

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	highestID  int // the highest id of all workers
	mapLock    sync.Mutex
	workersMap map[int]*WorkerConnection // map id to worker connection
	inCh       chan string
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
		highestID:  0, // worker ID allocation start from 0
		mapLock:    sync.Mutex{},
		workersMap: make(map[int]*WorkerConnection),
		inCh:       make(chan string),
	}
}

func (m *Master) ListenWorkers() {
	for {
		inMessage := <-m.inCh
		fmt.Println(inMessage)
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
	go m.ListenWorkers()
	// Accept incoming connections and handle them in a separate goroutine
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go m.HandleConnection(conn)
	}
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
}
