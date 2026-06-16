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
	DeadLitter
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
	DeadLitter atomic.Int64
	Failed atomic.Int64
	Retried atomic.Int64
}

