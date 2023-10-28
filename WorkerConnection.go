package main

import "net"

type WorkerConnection struct {
	Name string
	Addr string
	C    chan string
	conn net.Conn

	master *Master
}

func (c WorkerConnection) Run() {

}
