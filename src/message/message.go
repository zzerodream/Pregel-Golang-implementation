package message

type Message struct {
	SenderId int
	TargetId int
	Payload map[int]int
}


func NewMessage(sender int, target int, payload map[int]int) *Message {
	message := &Message {
		SenderId: sender,
		TargetId: target,
		Payload: payload,
	}
	return message
}
