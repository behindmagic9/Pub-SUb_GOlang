package broker

import (
	"observer/isubscriber"
	"observer/event"
	"observer/deliverystatus"
	"sync"
	"time"
	"sync/atomic"
)

type Broker struct{
	record map[string][]isubscriber.Isubscriber
	queue chan *deliverystatus.DeliveryTracker
	errQueue chan *deliverystatus.DeliveryTracker
	//	bufferQueue chan *event.Event
	metrics deliverystatus.Metrics
	closed atomic.Bool
	mu sync.RWMutex
	wg sync.WaitGroup
    wgBQ sync.WaitGroup
	wgPub sync.WaitGroup
	wgR sync.WaitGroup
	done chan struct{}
	closeOnce sync.Once
	startOnce sync.Once
	bufferQueue chan *deliverystatus.DeliveryTracker
}

// why we use struct{} cause its j:ust zero memory allocation and can be use to pass the signal only , that we needed right now

// var is not used inside the struct

const MAX_RETRY int = 5
const MAX_QUEUE_SIZE =100
const MAX_BUFFER_SIZE =200
const WORKERS = 5

func NewBroker() *Broker{
	return &Broker{
		record : make(map[string][]isubscriber.Isubscriber),
		queue : make(chan *deliverystatus.DeliveryTracker, MAX_QUEUE_SIZE),
		errQueue : make(chan *deliverystatus.DeliveryTracker,MAX_QUEUE_SIZE),
		closed : atomic.Bool{},
		done : make(chan struct{}),
		bufferQueue: make(chan *deliverystatus.DeliveryTracker, MAX_BUFFER_SIZE),
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
		Retry : 0,
		Status : deliverystatus.Queued,
	}
}

func (s *Broker) retry() {
	// reading from the channel
	backoff := 	func(retry int) time.Duration{
		return time.Duration(retry*retry) *10*time.Millisecond
	}
	/*
		if len(s.errQueue) <= 0{ // cause reading a nil channel is again blocking behvaiour
			return
		}
	*/
	// cant put this len check here cause its again a non-orderd code , so can publish immidetliy in concurrent environment without going through this
	for {
		select {
		case <- s.done:
			return
		case ev,ok := <-s.errQueue: // reading from errQueue
			if !ok{
				return
			}
			s.wgR.Add(1)
			cop := *ev
			tk := &cop
			time.AfterFunc(backoff(ev.Retry), func(){
				defer s.wgR.Done()
				select{
					case <-s.done:
						return
					default:
				}
				defer func(){
					if r := recover(); r!= nil{

					}
				}()
				select{
				case <-s.done:
					return
				case s.queue <- tk:
				default:
					s.metrics.Dropped.Add(1)
				}
			})
		}
	}
}

// Central Buffer handling
func (s *Broker) bufferHandling() {

	defer s.wgBQ.Done()

	for {
		select {
		case ev, ok := <-s.bufferQueue:
			if !ok {
				return
			}
			select {
			case s.queue <- ev:
			default:
				s.metrics.Dropped.Add(1)
			}
		}
	}
}

func (s *Broker) Start() {
	// just putting a check to check if cahnnel close then not call start again anyhow by mistake
	s.startOnce.Do(func() {

		if s.closed.Load() {
			return
		}

		// adding the buffer queue
		s.wgBQ.Add((1))
		go s.bufferHandling()

		for i := 0; i < WORKERS; i++ {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				// if len (s.queue ) <= 0{ // cause reading a nil channel is again blocking behvaiour
				// 	return
				// }
				// cant put this len check here cause its again a non-orderd code , so can publish immidetliy in concurrent environment without going through this
				// reading from the channel
				// == only have to process events no buffer nothing retry
				for {
					select {
					case ev, ok := <-s.queue:
						if !ok {
							return
						}
						s.ProcessEvents(ev)
					}
				}
			}()
		}

		s.wgR.Add(1)
		go func(){
			defer s.wgR.Done()
			s.retry()
		}()
	})
}

func (s *Broker) Notify(data *event.Event) {
	if s.closed.Load() { // if close then return , no more publishing
		return
	}
	s.wgPub.Add(1)
	defer s.wgPub.Done()

	s.mu.RLock()
	subs := s.record[data.Topic]
	s.mu.RUnlock()

	for _, sub := range subs{
		tk := s.encapsulate(data, sub)

		select{
			case <-s.done:
				return
			case s.queue <- tk:
				s.metrics.Published.Add(1)
			default:
				select{
					case s.bufferQueue<- tk:
						s.metrics.Published.Add(1)
					default:
						s.metrics.Dropped.Add(1)
				}
		}
	}
	// push the events into the queue
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	// i cant block this as this is helping in notifying or publishd
	// publishers too are running in parallel , and its harsh for them , to be get bock here by the lock
	// increment published here
	// here the queue channel also gets full and result in blocking to avoid that have to use the "Select"
	// as select is reading from both channel and see which ever is ready and write or read it , wihtout blokcing behaviour
//events there and run it in goroutine in parallel so that it will add them to the main queue
}

func(s *Broker) ProcessEvents(event *deliverystatus.DeliveryTracker){
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
func (s *Broker) evaluateEvents(first *deliverystatus.DeliveryTracker) {
	/*
		if len(s.bufferQueue) > 0 && len(s.queue) < MAX_QUEUE_SIZE/2{
			s.bufferQue()
		}
		first := s.queue[0]
	*/
/*	s.mu.RLock()
	subs := s.record[first.Event.Topic]
	subscribers := make([]isubscriber.Isubscriber, len(subs))
	copy(subscribers, subs)
	s.mu.RUnlock()
	*/
	first.Status = deliverystatus.Processing
	err := first.Subscriber.Update(first.Event) // as the Update gonna return the err
	if err != nil {
		first.Retry++
		first.Status = deliverystatus.Retrying
		// increment faied here
		s.metrics.Retried.Add(1)
		if first.Retry >= MAX_RETRY{
			first.Status = deliverystatus.DeadLetter
			s.metrics.DeadLetter.Add(1)
			return
		}
		select {
		case <-s.done:
			return
		case s.errQueue <- first:
		default:
			s.metrics.Dropped.Add(1)
			// === do nothing , cause i dont think the err queue will ever get full,even if that get full or channel close earlier the remain will drop those events
			// events there and run it in goroutine in parallel so that it will add them to the main queue
		}
	}else{
		first.Status = deliverystatus.Delivered
		// increment delivered here
		s.metrics.Delivered.Add(1)
	}
}

func (s *Broker) Unsubscribe(topic string, subb isubscriber.Isubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscribers := s.record[topic]
	for i,sb := range subscribers{
		if sb.GetID() == subb.GetID(){
			s.record[topic] = append(subscribers[:i],subscribers[i+1:]...)
			// appening/joining.. the observers underlying array froms start to previous elment of i and then next element of i to last
			break
		}
	}
}

func (s * Broker) GetMetrics() *deliverystatus.Metrics{
	return &s.metrics
}

func (s *Broker) closeChannel() {
	close(s.bufferQueue)
	s.wgBQ.Wait()
	close(s.queue)
	s.wg.Wait()
	close(s.errQueue)
	s.wgR.Wait()
}

func (s *Broker) Close() {
	// mutex lock so that no two thing can lock it simuntanoeusly
	s.closeOnce.Do(func() {

		// defer s.mu.Unlock() // can put the defer here cause now the worker will be waiting for the read lock(Rlock) there in evaluate_events and close will wait for workers to Done
		// instead have to release lock self
		s.mu.Lock()
		s.closed.Store(true)
		s.mu.Unlock()
		// will wait for 	// will wait for the this write and will release the lock now the this write and will release the lock now
		//  === Rule of this is  -> never hold mutex while calling Wait() ====
		close(s.done)
		s.wgPub.Wait()
		s.closeChannel()
	})
	//close(s.queue) // will shutdown the main queue first adn then wait for the workers to finish cause thats a timeout signal and they will finsih remian work and we wait for them to get out or finish with Wait()
	// wait here for letting all the gorutine number to finsh and return the done as they are running in parallel and wg is tracking there count
	// why here,because the wait is waiting for everything to be done and return and we cant close the cahnnel or goroutine whihc are processing the cahnnel
	//  thing otherwise channel close remain calling reading or writing from cahnnel result in deadlock/race condition
}
