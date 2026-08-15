package main

import (
	"context"
	"time"
	"math/rand/v2"
)

func main(){
	ctx, cancel := context.WithCancel(context.Background())

	m := &Metrics{}

	// Our central JobStore
	js := &JobStore{
		repository: make(map[int]*Job),
	}

	// Our central dispatcher
	d := &Dispatcher{
		workers: 3,
		jobs: make(chan *Job, 100),
		js: js,
		m: m,
	}

	d.Start(ctx)

	for i := 0; i < 10; i++ {
		which := rand.IntN(4)
		task := "nothing"
		p := Payload{"nothing"}
		switch which{
		case 0:
			task = "email_notification"
			p = Payload{"Emailing"}
		case 1:
			task = "resize_image"
			p = Payload{"2x"}
		case 2:
			task = "flaky_webhook_service"
			p = Payload{"Message: hai"}
		default:
			task = "failure"
			p = Payload{"failureeeee"}
		}
		// p := Payload{
		// 	JobName: "Haiiiii",
		// }
		job := &Job{
			id: i,
			Type: task,
			P: p,
			Status: Pending,
			MaxRetries: 10,
			CurrentRetries: 0,
			CreatedAt: time.Now(),
		}
		d.js.Save(job)
		d.Enqueue(job)
	}

	// Ensure order of operations so that workers can see context has been cancelled
	cancel()
	d.Stop()
}