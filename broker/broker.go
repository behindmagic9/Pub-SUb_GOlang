package broker 

import (
	"observer/isubscriber"
	"observer/event"
	"fmt"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
	queue []event.Event
	errQueue []DeliveryFail
	deadQueue []DeliveryFail
}
// var is not used inside the struct

const MAX_RETRY int = 4

type DeliveryFail struct{
	Event event.Event
	Subscriber isubscriber.Isubscriber
	Retry int
}

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : []event.Event{},
		errQueue : []DeliveryFail{},
		deadQueue  : []DeliveryFail{},
	}
}

func (s *Broker) Subscribe(topic string,obs isubscriber.Isubscriber) {
	s.record[topic] = append(s.record[topic], obs)
}

func (s *Broker) Notify(data event.Event) {
	// push the events into the queue
	s.queue = append(s.queue, data)
}

func(s *Broker) ProcessEvents(){
	if(len(s.queue) == 0) {return} 
	for(len(s.queue) > 0){
		s.evaluateEvents()
	}
	for(len(s.errQueue) > 0){
		s.evaluate_Failed_Events()
	}
}

// without adding the recieveer param will act like independent function not like memeber function of Broker
func (s *Broker) evaluateEvents(){
	first := s.queue[0]
	subscriber := s.record[first.Topic]
	for _,ob := range subscriber{
		err := ob.Update(first) // as the Update gonna return the err
		if err != nil{
			failure := DeliveryFail{
				Event : first,
				Subscriber : ob,
				Retry : 1,
			}
			s.errQueue = append(s.errQueue, failure)
		}
	}
	s.queue = s.queue[1:]
}

// without adding the recieveer param will act like independent function not like memeber function of Broker
func (s *Broker) evaluate_Failed_Events(){
	first := s.errQueue[0]

	err := first.Subscriber.Update(first.Event)
	if err != nil 
	{
		first.Retry++
		if first.Retry < MAX_RETRY{
			// log it cant process them and so dropping
			s.errQueue = append(s.errQueue, first)
		}else if  first.Retry >= MAX_RETRY {
			s.deadQueue = append(s.deadQueue, first)
			// can later implement logging here by loggin the complete imformation from the Delivery Struct of this
			fmt.Println("droping event as cant be able to process it after multiple retries")
		}
	}
	s.errQueue = s.errQueue[1:]
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
