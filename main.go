package main

import(
	"observer/subscriber"
	"observer/broker"
	"observer/event"
	"observer/publisher"
	"observer/deliverystatus"
	"fmt"
	"time"
	"sync"
)


func PrintMetrics(metrics *deliverystatus.Metrics) {
	fmt.Printf("Published %d \n", metrics.Published.Load())
	fmt.Printf("Delivered %d \n", metrics.Delivered.Load())
	fmt.Printf("DeadLetter %d \n", metrics.DeadLetter.Load())
	fmt.Printf("Retried %d \n", metrics.Retried.Load())
	fmt.Printf("Dropped %d \n", metrics.Dropped.Load())
}

func main(){

	var wg sync.WaitGroup

	obs1 := subscriber.NewOb("obs1")
	obs2 := subscriber.NewOb("obs2")

	subject := broker.NewBroker()
	subject.Subscribe("animal", obs1)
	subject.Subscribe("animal", obs2)
	subject.Subscribe("birds", obs1)
	subject.Subscribe("birds", obs2)


	publisher1 := publisher.NewPublisher("pub1", subject)

	publisher2 := publisher.NewPublisher("pub2", subject)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100{
			publisher1.Publish(event.NewEvent("birds" , "the brid fled",21))
			publisher2.Publish(event.NewEvent("animal" , "the animal run " , 23))
		}
	}()

	subject.Start() // cant let the start to be run in diff groutine for now cause that be difficult to debug rn 
	
	time.Sleep(100*time.Millisecond)
	wg.Wait()
	subject.Close()

	metrics := subject.GetMetrics()
	PrintMetrics(metrics)
}


// just remember to close the channel and when the channel are in blocking and nonblocking cause that can be causing the probelm 
// and confirm before closing channel if any goruotine associated with them is complete can be doneusing waitgroup
// and use of mutex for locking for read and write end , so that no else can read or write when u are , other alternative for varibale is atomic\
