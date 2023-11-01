package main

import (
	"os"
	"time"
	"fmt"
	"strconv"
)

func main() {
	args := os.Args
	fmt.Println(args[1])
	if args[1] == "master"{
	  // work as master
	  fmt.Println("Creating new master")
	  m := NewMaster()
	  m.Start()
	}
	if args[1] == "worker"{
		intValue, err := strconv.Atoi(os.Args[2])
	  	if err != nil {
		  	fmt.Println("Error:", err)
		  	return
	  	}
		// work as worker
		fmt.Printf("Creating new worker with ID %d\n",intValue)
	  	w := NewWorker(intValue)
	  	w.Run()
	}
	for {
	  time.Sleep(time.Second)
	}
  }