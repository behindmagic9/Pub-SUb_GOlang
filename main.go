package main

import(
	"observer/subscriber"
	"observer/broker"
	"observer/event"
	"observer/publisher"
)

func main(){
	obs1 := subscriber.NewOb("obs1")
	obs2 := subscriber.NewOb("obs2")

	subject := broker.NewBroker()
	subject.Subscribe("animal", obs1)
	subject.Subscribe("animal", obs2)
	subject.Subscribe("birds", obs1)
	subject.Subscribe("birds", obs2)

	e1 := event.NewEvent("animal","the animal run",23)

	e2 := event.NewEvent("birds", "the bids fled", 21)

	publisher1 := publisher.NewPublisher("pub1", subject)

	publisher2 := publisher.NewPublisher("pub2", subject)

	publisher1.Publish(e1)
	publisher2.Publish(e2)

	
	subject.ProcessEvents()
}
