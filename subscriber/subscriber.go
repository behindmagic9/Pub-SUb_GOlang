package subscriber   

import (
	"fmt"
	"observer/event"
	"observer/deliverystatus"
)

var globalcount = 0

type Subscriber struct{
	name string
	ID int
}

func NewOb(ob_name string) *Subscriber{
	globalcount++
	return &Subscriber{name  : ob_name,ID : globalcount}
}

func (s *Subscriber) Update(data *event.Event) error{
	data.Status = deliverystatus.Delivered
	fmt.Printf("this is the subscriber for this %s and here is the message %s price int is %d \n, status of the event is : %d", data.Topic, data.Message, data.Price, data.Status)
	return nil
}