package subscriber   

import (
	"fmt"
	"math/rand"
	"observer/event"
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
	if rand.Intn(3) == 0{
		return fmt.Errorf("simulate failure")
	}
//	fmt.Printf("this is the subscriber for this %s and here is the message %s price int is %d \n", data.Topic, data.Message, data.Price)
	return nil
}

func (s *Subscriber) GetID() int {
	return s.ID
} 
