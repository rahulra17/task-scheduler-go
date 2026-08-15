package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
)



func main(){
	ctx, stop := signal.NotifyContext(
        context.Background(), syscall.SIGINT, syscall.SIGTERM)

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

	router := http.NewServeMux()

	d.Start(ctx)

	router.HandleFunc("GET /jobs/{id}", d.GetJobHandler)

	router.HandleFunc("POST /jobs", d.CreateJobHandler)

	router.HandleFunc("GET /metrics", d.GetMetricsHandler)


	// Ensure order of operations so that workers can see context has been cancelled
	stop()
	d.Stop()
}