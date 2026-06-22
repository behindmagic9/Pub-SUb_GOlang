package deliverystatus

import(
	"observer/event"
	"observer/isubscriber"
	"sync/atomic"
)

type DeliveryStatus int

const (
	Queued DeliveryStatus = iota
	Processing
	Retrying
	DeadLetter
	Delivered
)

type DeliveryTracker struct{
	Event *event.Event
	Subscriber isubscriber.Isubscriber
	Retry int
	Status DeliveryStatus
}

type Metrics struct{
	Published atomic.Int64
	Delivered atomic.Int64
	DeadLetter atomic.Int64
	Retried atomic.Int64
	Dropped atomic.Int64
}

