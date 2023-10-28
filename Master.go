package main

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	mapLock    sync.Mutex
	workersMap map[string]*WorkerConnection // map ip addr to worker connection
	inCh       chan string
}

func (m *Master) RegisterWorker(conn net.Conn) *WorkerConnection {
	m.mapLock.Lock()
	wc := &WorkerConnection{
		Addr:   conn.RemoteAddr().String(),
		C:      make(chan string),
		conn:   conn,
		master: m,
	}
	m.workersMap[wc.Addr] = wc
	m.mapLock.Unlock()
	return wc
}

func NewMaster() *Master {
	return &Master{
		mapLock:    sync.Mutex{},
		workersMap: make(map[string]*WorkerConnection),
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
	graph := ParseInput("./")
	fmt.Println(graph)
	partitions := Partition([]Vertex{}, len(m.workersMap))
	index := 0
	for _, connection := range m.workersMap {
		connection.C <- fmt.Sprint(partitions[index])
		index++
	}

}
