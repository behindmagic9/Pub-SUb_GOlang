package main

import(
	"observer/subscriber"
	"observer/broker"
	"observer/event"
)

func main(){
	obs1 := subscriber.NewOb("obs1")
	obs2 := subscriber.NewOb("obs2")

	subject := broker.NewBroker()
	subject.Subscribe("animal", obs1)
	subject.Subscribe("animal", obs2)
	subject.Subscribe("birds", obs1)
	subject.Subscribe("birds", obs2)

	e1 := event.Event{ "animal","the animal run",23}

	e2 := event.Event{ "birds", "the bids fled", 21}

	subject.Notify("animal", e1)
	subject.Notify("birds", e2)	
}
