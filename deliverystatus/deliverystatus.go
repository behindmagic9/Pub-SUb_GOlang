package deliverystatus

import(
	"observer/event"
	"observer/isubscriber"
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
	Published int
	Delivered int
	DeadLitter int
	Failed int
	Retried int
}

