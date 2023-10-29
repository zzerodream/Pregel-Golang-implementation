package message

type Message struct {
	From int
	To int
	Value interface{}
	Type int
}


func NewMessage(sender int, target int, payload interface{}, t int) *Message {
	message := &Message {
		From: sender,
		To: target,
		Value: payload,
		Type: t,
	}
	return message
}
