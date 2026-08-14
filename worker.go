package main

import (
	"fmt"
	"time"
	"sync"
	"context"
)

type Payload struct {
	JobName string `json:"jobName"`
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
	js *JobStore
}

func execute(job *Job, js *JobStore) bool{
	fmt.Println("Doing Work! This is the job: ", job.id)
	js.UpdateStatus(job.id, Processing)
	fmt.Println("Job has now completed: ", job.Status)
	return true
}

func (d *Dispatcher) worker(ctx context.Context) {
	defer d.wg.Done()
	for job := range d.jobs {
		execute(job, d.js)
		// This is safe because it won't be touched by another worker until we put it onto job queue
		value,_ := d.js.ReadJob(job.id)
		if value.Status == Failed {
			d.js.UpdateRetries(job.id)
			if job.CurrentRetries < job.MaxRetries {
				time.Sleep(100*time.Millisecond) // Small Delay to register if context has been cancelled while relaxing job queue
				select {
					case <-ctx.Done():
						fmt.Println("Channel Stopped, Job won't be done")
					case d.jobs <- job:
						fmt.Println("Retrying job: ", job.id)
				}
			} else {
				fmt.Println("Job failed and past maximum retries")
			}
		}
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	for i := 0; i < d.workers; i++ {
		d.wg.Add(1)
		go d.worker(ctx)
	}
}

func (d *Dispatcher) Enqueue(job *Job) {
	d.jobs <- job
}

func (d *Dispatcher) Stop() {
	close(d.jobs)
	d.wg.Wait()
}

// Storage for all Jobs for visibility across workers
type JobStore struct {
	repository map[int]*Job;
	lock sync.RWMutex
}

// Saves a job to the jobstore
func (j *JobStore) Save(job *Job) {
	// lock
	j.lock.Lock()
	j.repository[job.id] = job
	j.lock.Unlock()
}

// Safely increment status
func (j *JobStore) UpdateStatus(id int, status Status) {
	j.lock.Lock()
	j.repository[id].Status = status
	j.lock.Unlock()
}

// Function to increment retries in jobStore safely
func (j *JobStore) UpdateRetries(id int) {
	j.lock.Lock()
	j.repository[id].CurrentRetries++
	j.lock.Unlock()
}

func (j *JobStore) ReadJob(id int) (*Job, bool) {
	j.lock.RLock()
	defer j.lock.RUnlock()

	value, exists := j.repository[id]

	return value, exists
}



