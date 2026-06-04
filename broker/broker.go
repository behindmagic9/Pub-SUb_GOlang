package broker

import (
	"observer/isubscriber"
	"observer/event"
	"observer/deliverystatus"
	"fmt"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
	queue []*event.Event
	errQueue []*deliverystatus.DeliveryTracker
	deadQueue []*deliverystatus.DeliveryTracker
	bufferQueue []*event.Event
	metrics deliverystatus.Metrics
}
// var is not used inside the struct

const MAX_RETRY int = 4
const MAX_QUEUE_SIZE =10

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : []*event.Event{},
		errQueue : []*deliverystatus.DeliveryTracker{},
		deadQueue  : []*deliverystatus.DeliveryTracker{},
		bufferQueue : []*event.Event{},
	}
}

func (s *Broker) Subscribe(topic string,obs isubscriber.Isubscriber) {
	s.record[topic] = append(s.record[topic], obs)
}

func (s *Broker) encapsulate(data *event.Event, sb isubscriber.Isubscriber) *deliverystatus.DeliveryTracker{
	return &deliverystatus.DeliveryTracker{
		Event : data,
		Subscriber : sb,
		Retry : 1,
		Status : deliverystatus.Retrying,
	}
}

func (s *Broker) Notify(data *event.Event) {
	// push the events into the queue
	// increment published here
	s.metrics.Published++
	if(len(s.queue) >= MAX_QUEUE_SIZE){
		s.bufferQueue = append(s.bufferQueue, data)
	}else{
		s.queue = append(s.queue, data)
	}
}

func(s *Broker) ProcessEvents(){
	for(len(s.queue) > 0){
		s.evaluateEvents()
	}
	for(len(s.errQueue) > 0){
		s.evaluate_Failed_Events()
	}
}

func (s *Broker) bufferQue(){
	for len(s.queue) < MAX_QUEUE_SIZE/2 && len(s.bufferQueue) > 0{
		data := s.bufferQueue[0]
		s.queue = append(s.queue,data) 
		s.bufferQueue = s.bufferQueue[1:]
	}
}

// without adding the recieveer param will act like independent function not like memeber function of Broker
func (s *Broker) evaluateEvents(){
	if len(s.bufferQueue) > 0 && len(s.queue) < MAX_QUEUE_SIZE/2{
		s.bufferQue()
	}
	first := s.queue[0]
	subscriber := s.record[first.Topic]
	for _,sb := range subscriber{
		err := sb.Update(first) // as the Update gonna return the err
		if err != nil{
			failure := s.encapsulate(first, sb)
			// increment faied here
			s.metrics.Failed++
			s.errQueue = append(s.errQueue, failure)
		}else{
			// increment delivered here
			s.metrics.Delivered++
		}
	}
	s.queue = s.queue[1:]
}

// without adding the reciever param will act like independent function not like memeber function of Broker
func (s *Broker) evaluate_Failed_Events(){
	if len(s.errQueue) == 0 {return}
	first := s.errQueue[0]
	s.errQueue = s.errQueue[1:]
	err := first.Subscriber.Update(first.Event)
	if err != nil {
		first.Retry++
		if first.Retry < MAX_RETRY{
			// log it cant process them and so dropping
			// increment retriied here
			s.metrics.Retried++
			s.errQueue = append(s.errQueue, first)
		}else {
			first.Status = deliverystatus.DeadLitter
			// increment dead letter here
			s.metrics.DeadLitter++
			s.deadQueue = append(s.deadQueue, first)
			// can later implement logging here by loggin the complete imformation from the Delivery Struct of this
			fmt.Println("droping event as cant be able to process it after multiple retries")
		}
	}else{
		// increment delivered here
		s.metrics.Delivered++
	}
}

func (s *Broker) Unsubscribe(topic string, subb isubscriber.Isubscriber){
	subscribers := s.record[topic]
	for i,sb := range subscribers{
		if(sb.GetID() == subb.GetID()){
			s.record[topic] = append(subscribers[:i],subscribers[i+1:]...)
			// appening/joining.. the observers underlying array froms start to previous elment of i and then next element of i to last
			break
		}
	}
}


func (s * Broker) GetMetrics() deliverystatus.Metrics{
	return &s.metrics
}