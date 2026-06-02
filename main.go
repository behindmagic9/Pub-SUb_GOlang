package main

import(
	"observer/subscriber"
	"observer/broker"
)

func main(){
	obs1 := subscriber.NewOb("obs1")
	obs2 := subscriber.NewOb("obs2")

	subject := broker.NewBroker()
	subject.Subscribe("animal", obs1)
	subject.Subscribe("animal", obs2)
	subject.Subscribe("birds", obs1)
	subject.Subscribe("birds", obs2)

	subject.Notify("animal", "lion")
	subject.Notify("birds", "eagle")	
}
