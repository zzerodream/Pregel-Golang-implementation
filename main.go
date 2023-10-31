package main

import (
	"os"
	"time"
	"fmt"
)

func main() {
	args := os.Args
	fmt.Println(args[1])
	if args[1] == "master" {
	  // work as master
	  fmt.Println("Creating new master")
	  m := NewMaster()
	  m.Start()
	}
	if args[1] == "worker" {
	  // work as worker
	  fmt.Println("Creating new worker ")
	  w := NewWorker()
	  w.Run()
	}
	for {
	  time.Sleep(time.Second)
	}
  }