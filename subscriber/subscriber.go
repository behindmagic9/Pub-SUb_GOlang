package subscriber   

import (
	"fmt"
	"observer/event"
)

type Subscriber struct{
	name string	
}

func NewOb(ob_name string) *Subscriber{
	return &Subscriber{name  : ob_name}
}

func (s *Subscriber) Update(topic string, data event.Event){
	fmt.Printf("this is the subscriber for this %s and here is the message %s price int is %d \n", topic, data.Message, data.Price)
}
