package main

import (
	"fmt"
	"sync"
	"uuid"
	"context"
	"time"
)

func main(){
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel
	d := &Dispatcher{
		workers: 3,
		jobs: make(chan Job, 100),
	}

	d.Start(ctx)

	for i := 0; i < 10; i++ {
		job := &Job{
			id: i,
			Type: "dummy",
			P: []byte(`{jobName: "haiii"}`),
			Status: Pending,
			MaxRetries: 10,
			CurrentRetries: 0,
			CreatedAt: time.Now(),
		}
		d.Enqueue(job)
	}
	d.Stop()
}