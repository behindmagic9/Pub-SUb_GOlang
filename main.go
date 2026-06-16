package main

import(
	"observer/subscriber"
	"observer/broker"
	"observer/event"
	"observer/publisher"
	"observer/deliverystatus"
	"fmt"
	"time"
)


func PrintMetrics(metrics *deliverystatus.Metrics) {
	fmt.Printf("Published %d \n", metrics.Published.Load())
	fmt.Printf("Delivered %d \n", metrics.Delivered.Load())
	fmt.Printf("DeadLitter %d \n", metrics.DeadLitter.Load())
	fmt.Printf("Failed %d \n", metrics.Failed.Load())
	fmt.Printf("Retried %d \n", metrics.Retried.Load())
}

func main(){
	obs1 := subscriber.NewOb("obs1")
	obs2 := subscriber.NewOb("obs2")

	subject := broker.NewBroker()
	subject.Subscribe("animal", obs1)
	subject.Subscribe("animal", obs2)
	subject.Subscribe("birds", obs1)
	subject.Subscribe("birds", obs2)


	publisher1 := publisher.NewPublisher("pub1", subject)

	publisher2 := publisher.NewPublisher("pub2", subject)

	
	go func() {
		for range 10{
			publisher1.Publish(event.NewEvent("birds" , "the brid fled",21))
			publisher2.Publish(event.NewEvent("animal" , "the animal run " , 23))
		}
	}()

	go subject.Start()
	
	time.Sleep(time.Second)
	subject.Close()

	metrics := subject.GetMetrics()
	PrintMetrics(metrics)
	
}
