package main

import (
	"fmt"
	"time"
	"sync"
	"context"
	"encoding/json"
)

type Payload struct {
	jobName string `json:"jobName"`
}

type Status int

const (
	Completed Status = iota
	Pending
	Processing
	Failed
)

type Job struct {
	id int
	Type string
	P Payload
	Status Status
	MaxRetries int
	CurrentRetries int
	CreatedAt time.Time
	CompletedAt time.Time
}

type Dispatcher struct {
	workers int
	jobs chan *Job // Create a channel to send jobs through
	wg sync.WaitGroup // Create a Wait Group for workers to synchronize upon cancellation
}

func (d *Dispatcher) Start(ctx context.Context) {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for job := range d.jobs{
				fmt.Println("Working on: ", job.id)
				job.Status = Processing
				doWork(job)
				// Job has failed
				if job.Status == Failed {
					job.CurrentRetries++
					if job.CurrentRetries < job.MaxRetries {
						time.Sleep(100*time.Millisecond) // Small Delay to register if context has been cancelled and relax job queue
						select {
							case <-ctx.Done():
								fmt.Println("Channel Stopped, Job won't be done")
							case d.jobs <- job:
								fmt.Println("Retrying job: ", job.id)
						}
					}
					fmt.Println("Job past maximum retries")
				}
			}
		}()
	}
}

func (d *Dispatcher) Enqueue(job *Job) {
	d.jobs <- job
}

func (d *Dispatcher) Stop() {
	close(d.jobs)
	d.wg.Wait()
}