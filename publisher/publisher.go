package publisher

import (
	"observer/broker"
	"observer/event"
	"observer/deliverystatus"
)

type Publisher struct{
	name string
	broker *broker.Broker
}

func (p *Publisher) Publish(e *event.Event){
	e.Status = deliverystatus.Publishing
	p.broker.Notify(e) // e already is-a pointer
}

func NewPublisher(pub_name string, brok *broker.Broker) *Publisher{
	return &Publisher{name : pub_name, broker : brok}
}
