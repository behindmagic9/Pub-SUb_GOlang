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
	deadQueue []*deliverystatus.DeliveryTracker
//	bufferQueue chan *event.Event
	metrics deliverystatus.Metrics
	closed atomic.Bool
	mu sync.RWMutex
	wg sync.WaitGroup
	done chan struct{}
}
// why we use struct{} cause its just zero memory allocation and can be use to pass the signal only , that we needed right now

// var is not used inside the struct

const MAX_RETRY int = 4
const MAX_QUEUE_SIZE =10

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : make(chan *event.Event, MAX_QUEUE_SIZE),
		errQueue : make(chan *deliverystatus.DeliveryTracker,MAX_QUEUE_SIZE),
		deadQueue  : []*deliverystatus.DeliveryTracker{},
	//	bufferQueue : []*event.Event{},
		closed : atomic.Bool{},
		done : make(chan struct{})
	}
}

func (s *Broker) Subscribe(topic string,obs isubscriber.Isubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	// reading from the channel
	defer s.wg.Done()
	/*
	if len(s.errQueue) <= 0{ // cause reading a nil channel is again blocking behvaiour
		return
	}
	*/
	// cant put this len check here cause its again a non-orderd code , so can publish immidetliy in concurrent environment without going through this
	for ev := range s.errQueue { 
		s.evaluate_Failed_Events(ev)
	}
}

func (s *Broker) Start() {
	s.wg.Add(1) // putting the count of adding of number og goroutine outside that start for now
	go s.retry()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// if len (s.queue ) <= 0{ // cause reading a nil channel is again blocking behvaiour
		// 	return
		// }
		// cant put this len check here cause its again a non-orderd code , so can publish immidetliy in concurrent environment without going through this
		// reading from the channel
		for ev := range s.queue {
			s.ProcessEvents(ev)
		}
		close(s.errQueue)
	}()
}

func (s *Broker) Notify(data *event.Event) {
	// push the events into the queue
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	// i cant block this as this is helping in notifying or publishd
	// publishers too are running in parallel , and its harsh for them , to be get bock here by the lock 
	if s.closed.Load() { // if close then return , no more publishing
		return 
	}
	// increment published here
	s.metrics.Published.Add(1)
	// here the queue channel also gets full and result in blocking to avoid that have to use the "Select"
	// as select is reading from both channel and see which ever is ready and write or read it , wihtout blokcing behaviour
	select{
		// have to use done cause removed the lock from the notify and that can cause panic as will push event in channel
		case <-s.done: // if done or signalled from done, that mean closed and just return
			return
		case s.queue <- data : 		 // write to channel
		default :
			s.metrics.DeadLitter.Add(1)
			// in future will introduce the buffer which will store the
			// events there and run it in goroutine in parallel so that it will add them to the main queue 
	}
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
	s.mu.RLock()
	subscriber := s.record[first.Topic]
	s.mu.RUnlock()
	for _,sb := range subscriber{
		err := sb.Update(first) // as the Update gonna return the err
		if err != nil{
			failure := s.encapsulate(first, sb)
			// increment faied here
			s.metrics.Failed.Add(1)
			
			select{
				case s.errQueue<- failure // write to channel
				default :
					s.metrics.DeadLitter.Add(1)	// adding to the deadlitter for now , in future will introduce the buffer which will store the
					// events there and run it in goroutine in parallel so that it will add them to the main queue 
			}
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
			// here the err queue channel also gets full and result in blocking to avoid that have to use the "Select"
			select{
				case s.errQueue<- first : 		 // write to channel
				default :
					s.metrics.DeadLitter.Add(1)	// adding to the deadlitter for now , in future will introduce the buffer which will store the
					// events there and run it in goroutine in parallel so that it will add them to the main queue 
			}
		}else {
			first.Status = deliverystatus.DeadLitter
			// increment dead letter here
			s.metrics.DeadLitter.Add(1)
			// removing the dead queue channel cause thats unnecessary , we can put that in queue
			// s.deadQueue<- first
			// temp := <- s.deadQueue
			s.mu.Lock()
			s.deadQueue = append(s.deadQueue, first)
			s.mu.Unlock()
			// can later implement logging here by loggin the complete imformation from the Delivery Struct of this
			fmt.Printf("droping event as cant be able to process it after multiple retries with Id %d \n" , first.Event.ID)
		}
	}else{
		// increment delivered here
		s.metrics.Delivered.Add(1)
	}
}

func (s *Broker) Unsubscribe(topic string, subb isubscriber.Isubscriber){
	s.mu.Lock()
	defer s.mu.Unlock()
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
	// mutex lock so that no two thing can lock it simuntanoeusly
	s.mu.Lock()
	// defer s.mu.Unlock() // can put the defer here cause now the worker will be waiting for the read lock(Rlock) there in evaluate_events and close will wait for workers to Done
	// instead have to release lock self
	if s.closed.Load() { // if closed is true , close the cahnnel , that mean if agagin trying to close then return cause already close
		s.mu.Unlock() // release lock as returning now
		return
	}
	s.closed.Store(true)
	// will wait for the this write and will release the lock now
	//  === Rule of this is  -> never hold mutex while calling Wait() ====
	s.mu.Unlock()
	close(s.done)
	close(s.queue) // will shutdown the main queue first adn then wait for the workers to finish cause thats a timeout signal and they will finsih remian work and we wait for them to get out or finish with Wait()
	s.wg.Wait()
	 // wait here for letting all the gorutine number to finsh and return the done as they are running in parallel and wg is tracking there count
	// why here,because the wait is waiting for everything to be done and return and we cant close the cahnnel or goroutine whihc are processing the cahnnel
	//  thing otherwise channel close remain calling reading or writing from cahnnel result in deadlock/race condition
}