package main

import (
	"os"
	"time"
)

func main() {
	args := os.Args
	if args[1] == "master" {
		// work as master
		m := NewMaster()
		m.Start()
	}
	for {
		time.Sleep(time.Second)
	}
}
