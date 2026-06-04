package event

var globalCounter int

type Event struct{
	ID int
	Topic string
	Message string
	Price int
}

func NewEvent(topic string, message string, price int) *Event{
	globalCounter++
	return &Event{
		ID : globalCounter,
		Topic : topic,
		Message : message,
		Price : price,
	}
}
