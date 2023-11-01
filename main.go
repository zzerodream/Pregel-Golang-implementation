package main

import (
	"os"
	"time"
	"fmt"
	"strconv"
)

var IPADD = map[int]string{
    1: "192.168.56.104",
    2: "192.168.56.105",
}

var IPADD_R = map[string]int{
    "192.168.56.104": 1,
    "192.168.56.105": 2,
}

var MASTERIP = "192.168.56.106"
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