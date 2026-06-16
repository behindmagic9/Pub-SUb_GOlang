package broker

import (
	"observer/isubscriber"
	"observer/event"
	"observer/deliverystatus"
	"fmt"
	"sync"
	"sync/atomic"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
	queue chan *event.Event
	errQueue chan *deliverystatus.DeliveryTracker
	deadQueue chan *deliverystatus.DeliveryTracker
//	bufferQueue chan *event.Event
	metrics deliverystatus.Metrics
	closed atomic.Bool
	mu sync.Mutex
}
// var is not used inside the struct

const MAX_RETRY int = 4
const MAX_QUEUE_SIZE =10

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : make(chan *event.Event, MAX_QUEUE_SIZE),
		errQueue : make(chan *deliverystatus.DeliveryTracker,MAX_QUEUE_SIZE),
		deadQueue  : make(chan *deliverystatus.DeliveryTracker, MAX_QUEUE_SIZE),
	//	bufferQueue : []*event.Event{},
		closed : atomic.Bool{},
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

func (s *Broker) retry() {
	for ev := range s.errQueue {
		s.evaluate_Failed_Events(ev)
	}
	close(s.deadQueue)
}

func (s *Broker) Start() {
	go s.retry()
	go func() {
		for ev := range s.queue {
			s.ProcessEvents(ev)
		}
		close(s.errQueue)
	}()
}

func (s *Broker) Notify(data *event.Event) {
	// push the events into the queue
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return 
	}
	// increment published here
	s.metrics.Published.Add(1)
	s.queue <- data
}

func(s *Broker) ProcessEvents(event *event.Event){
	// cant check multiple times as on recivieng the event we only get here so after retrieivn that event , if the queue contians only one elemtn became null now so check of len greater than zero wont pass
	s.evaluateEvents(event)
}
/* commenting for now
func (s *Broker) bufferQue(){
	for len(s.queue) < MAX_QUEUE_SIZE/2 && len(s.bufferQueue) > 0{
		data := s.bufferQueue[0]
		s.queue = append(s.queue,data) 
		s.bufferQueue = s.bufferQueue[1:]
	}
}
*/

// without adding the recieveer param will act like independent function not like memeber function of Broker
func (s *Broker) evaluateEvents(first *event.Event){
	/*
	if len(s.bufferQueue) > 0 && len(s.queue) < MAX_QUEUE_SIZE/2{
		s.bufferQue()
	}
	first := s.queue[0]
	*/
	subscriber := s.record[first.Topic]
	for _,sb := range subscriber{
		err := sb.Update(first) // as the Update gonna return the err
		if err != nil{
			failure := s.encapsulate(first, sb)
			// increment faied here
			s.metrics.Failed.Add(1)
			s.errQueue<- failure
		}else{
			// increment delivered here
			s.metrics.Delivered.Add(1)
		}
	}
}

// without adding the reciever param will act like independent function not like memeber function of Broker
func (s *Broker) evaluate_Failed_Events(first *deliverystatus.DeliveryTracker){
/*
	if len(s.errQueue) == 0 {return}
	first := s.errQueue[0]
	s.errQueue = s.errQueue[1:]
*/
	err := first.Subscriber.Update(first.Event)
	if err != nil {
		first.Retry++
		if first.Retry < MAX_RETRY{
			// log it cant process them and so dropping
			// increment retriied here
			s.metrics.Retried.Add(1)
			s.errQueue<- first
		}else {
			first.Status = deliverystatus.DeadLitter
			// increment dead letter here
			s.metrics.DeadLitter.Add(1)
			s.deadQueue<- first
			temp := <- s.deadQueue
			// can later implement logging here by loggin the complete imformation from the Delivery Struct of this
			fmt.Printf("droping event as cant be able to process it after multiple retries with Id %d \n" , temp.Event.ID)
		}
	}else{
		// increment delivered here
		s.metrics.Delivered.Add(1)
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


func (s * Broker) GetMetrics() *deliverystatus.Metrics{
	return &s.metrics
}

func (s *Broker) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return
	}
	s.closed.Store(true)
	close(s.queue)
}
