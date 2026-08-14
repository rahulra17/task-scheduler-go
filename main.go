package main

import (
	"context"
	"time"
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
		p := Payload{
			JobName: "Haiiiii",
		}
		job := &Job{
			id: i,
			Type: "dummy",
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