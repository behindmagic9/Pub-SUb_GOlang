package broker 

import (
	"observer/isubscriber"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
}
// var is not used inside the struct

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
	}
}

func (s *Broker) Subscribe(topic string,obs isubscriber.Isubscriber) {
	s.record[topic] = append(s.record[topic], obs)
}

func (s *Broker) Notify(name string, data any) {
	subscriber := s.record[name]
	for _,ob := range subscriber{
		ob.Update(name, data)
	}
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
