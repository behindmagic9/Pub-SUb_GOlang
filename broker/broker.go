package broker 

import (
	"observer/isubscriber"
	"observer/event"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
	queue []event.Event
}
// var is not used inside the struct

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : []event.Event{},
	}
}

func (s *Broker) Subscribe(topic string,obs isubscriber.Isubscriber) {
	s.record[topic] = append(s.record[topic], obs)
}

func (s *Broker) Notify(data event.Event) {
	// push the events into the queue
	append(s.queue, data)
}

func(s *Broker) ProcessEvents(){
	if(len(s.queue) == 0) {return} 
	for(len(s.queue) > 0)
		EvaluateEvents()
}

func evaluate_events(){
	first := s.queue[0]
	subscriber := s.record[first.Topic]
		for _,ob := range subscriber{
		ob.Update(first)
	}
	s.queue := s.queue[1:]
}


func (s *Broker) Unsubscribe(topic string, obs isubscriber.Isubscriber){
	subscribers := s.record[topic]
	for i,ob := range subscribers{
		if(ob == obs){
			s.record[topic] = append(subscribers[:i],subscribers[i+1:]...)
			// appening/joining.. the observers underlying array froms start to previous elment of i and then next element of i to last
			break
		}
	}
}
