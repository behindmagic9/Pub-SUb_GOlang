package subscriber   

import (
	"fmt"
)

type Subscriber struct{
	name string	
}

func NewOb(ob_name string) *Subscriber{
	return &Subscriber{name  : ob_name}
}

func (s *Subscriber) Update(topic string, data any){
	fmt.Println("this is the subscriber for this " + topic)
}
