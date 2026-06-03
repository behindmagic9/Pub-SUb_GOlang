package deliverystatus


type DeliveryStatus int

const (
	Initialized DeliveryStatus = iota
	Publishing
	Queued
	BufferQ
	Processing
	Retrying
	DeadLitter
	Delivered
)