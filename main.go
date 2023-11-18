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
	  intValue, _ := strconv.Atoi(os.Args[2])
	  fmt.Printf("Creating new master with ID %d\n", intValue)
	  m := NewMaster(intValue)
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