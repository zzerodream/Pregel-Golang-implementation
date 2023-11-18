package main

type Type int
const (
	START_NEXT Type = iota
	COMPUTE_FINISH
	SEND_FINISH
	SEND_EMPTY
	EXCHANGE_START
	EXCHANGE
	ASSIGN_VERTEX
	ASSIGN_FINISHED
	EXCHANGE_STOP
	EXIT
	HEARTBEAT
	RESTART
	RESTART_STATE
)

/*message
	type 0 StartSuperstep --> "Master send out messages to inform worker to proceed next superstep"
	type 1 ComputeFinished --> "Worker has finished all the vertex compute function under it, inform Master"
	type 2 Sent  --> Worker has finished sending out all outgoing messages.
	type 3 Empty   --> WWorker's outgoing queue is empty, there's no new message
	type 4 StartExchange --> "Master inform worker to start exchanging messages and handle incoming message"
	type 5 ExchangeMessage --> "Normal exchange message between workers"
	type 6 AssignPartition → “Master send vertex infos to worker at the beginning”
	Type 7 PartitionSent → “Master inform worker it has sent all partitions to it”
	Type 8 ExchangeStopped. -> “Master inform worker to stop exchange message including handle incoming and outgoing messages”
	Type 9 EXIT --> Algo finished, exit
	Type 10 HeartBeat --> "Worker sends a message indicating it is alive to master"
*/
type Message struct {
	From int
	To int
	Value interface{}
	Type Type
}


func NewMessage(sender int, target int, payload interface{}, t Type) *Message {
	message := &Message {
		From: sender,
		To: target,
		Value: payload,
		Type: t,
	}
	return message
}