package main
import (
    "sync"
	"net"
)

/*Worker initialization and things to do after initialization

	7. workers call ReceiveGraphPartition(), will close once receive all partitions
	8. Then workers can use a go routine to call ReceiveFromMaster() for other instructions from master. 

*/

// call all necessary functions for the worker
func (w *Worker) Run() {
	var wg sync.WaitGroup
	//Establish connection with master
	w.EstablishMasterConnection()  //not using go routine cause I want to make sure the connection is establish before proceeding to next step
	//while workers are trying to e currently will keep running and not killedstablish connection between each other, we can start receving partitions from the master
	wg.Add(1)
	go func(){
		w.ReceiveGraphPartition()
		wg.Done()
	}()
	//Start listener and try to establish connection with other workers
	wg.Add(1)
	go func(){
		w.StartListener()  // Begin listening for incoming connections in a new go routine, will stop once all connections are established
		wg.Done()
	}()
	wg.Add(1)
	go func(){
		w.ConnectToWorkerssWithLowerID()
		wg.Done()
	}()
	// only proceed after receving all partitions and establish all connections
	wg.Wait()
	//continue receiving instructions from master.
	w.ReceiveFromMaster()

}
//maybe write in a way to let user pass ID through command line go run file.go ID, masterIP, port, sth like this
func main_for_Worker(){ 
	/* Create a new worker instance
		a. make a large enough workerChan
		b. set the worker ID
		c. Initialize all maps and slice
		d. initialize the mutex lock
		e. things to hard code for now: IPs and reverse_IPs, numberofWorkers
	*/
	worker := &Worker{
		ID:             1,
		Vertices:       make(map[int]*Vertex),
		MessageQueue:   []Message{},
		workerChan:     make(chan *Message,100),
		IPs:            make(map[int]string),
		reverse_IPs:    make(map[string]int),
		Connections:    make(map[int]net.Conn),
		mutex:          sync.Mutex{},
		numberOfWorkers:  5, // set number of workers,
		MasterIP: "IP", //set the IP of the msater
		MasterPort: 11111,
	}

	// Run the worker
	worker.Run()
}
