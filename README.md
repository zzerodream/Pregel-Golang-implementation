# Mini Pregel


A Go implementation of Google's distributed large graph processing system - Pregel

Things I have modified for Vertex.go:  
	1. for MessageChan, use a slice of Message instead of channel, and rename it as IncomingMessages  
	2. When initializing the vertex, the initial state is IDLE.   
	3. initializing the channel will be done at the worker side.  
	4. All the vertices share the same MessageChan with the worker.  
	5. Rename SendMessageToServer as SendMessageToWorker  


Other modifications:  
    1. Rename MessageTypes.go as Message.go, includes the defination for message struct and update the MessageType.  
    2. update Worker.go  
    3. Functions to establish connections are all under Worker.go, maybe can delete WorkerConnection.go  


Things to do:  
- How does the master know when to start the first superstep after workers receive the partition (extra message type or just a short timeout after sending the partitions)

- 
