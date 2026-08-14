package main

import (
	"fmt"
	"time"
	"sync"
	"context"
	"sync/atomic"
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
	m *Metrics
}

func execute(job *Job, js *JobStore, m *Metrics) bool{
	if job, ok := js.ReadJob(job.id); ok {
		fmt.Println("Doing Work! This is the job: ", job.id)
		init_status := job.Status
		js.UpdateStatus(job.id, Processing)
		m.Active.Add(1)
		fmt.Println("Job has now completed: ", job.id)
		// check to see if it was a failed job so we can update the status
		if init_status == Failed {
			defer m.Failed.Add(-1)
		}

		// Need to load up values since they are atomic
		fmt.Println("Active: ", m.Active.Load())
		fmt.Println("Failed: ", m.Failed.Load())
		fmt.Println("Completed: ", m.JobsDone.Load())

		js.PrintJobs()

		// TODO Actually have a switch statement here, will do later that sets failure or success
		// the switch statement arugment needs to return if it failed or suceeded
		job,_ = js.ReadJob(job.id)
		// Update values safely so we can return true
		defer m.Active.Add(-1)
		if job.Status != Failed{
			m.JobsDone.Add(1)
			js.UpdateStatus(job.id, Completed)
			return true
		} else{
			m.Failed.Add(1)
			js.UpdateStatus(job.id, Failed)
		}
	}
	return false
}

func (d *Dispatcher) worker(ctx context.Context) {
	defer d.wg.Done()
	for job := range d.jobs {
		execute(job, d.js, d.m)
		// This is safe because it won't be touched by another worker until we put it onto job queue
		value,ok := d.js.ReadJob(job.id)
		if ok && value.Status == Failed {
			CurrentRetries := d.js.UpdateRetries(job.id)
			// MaxRetries will never change so this is safe to read without locking
			if CurrentRetries < job.MaxRetries {
				time.Sleep(100*time.Millisecond) // Small Delay to register if context has been cancelled while relaxing job queue

				// Safe check to see everything
				if ctx.Err() != nil {
					return
				}
				select {
					case <-ctx.Done():
						fmt.Println("Channel Stopped, Job won't be done")
					case d.jobs <- job:
						d.js.UpdateStatus(job.id, Pending)
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
	repository map[int]*Job
	lock sync.RWMutex
}

// struct for global metrics
type Metrics struct {
	JobsDone atomic.Int64
	Active atomic.Int64
	Failed atomic.Int64
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
func (j *JobStore) UpdateRetries(id int) int{
	j.lock.Lock()
	defer j.lock.Unlock()
	j.repository[id].CurrentRetries++
	return j.repository[id].CurrentRetries
}

func (j *JobStore) ReadJob(id int) (*Job, bool) {
	j.lock.RLock()
	defer j.lock.RUnlock()

	value, exists := j.repository[id]

	return value, exists
}

func (j *JobStore) PrintJobs() {
	j.lock.RLock()
	for id, job := range j.repository{
		for range 50{
			fmt.Print("-")
		}
		fmt.Println()
		fmt.Println("Job Id:", id)
		fmt.Println("Job: ", job)
		for range 50{
			fmt.Print("-")
		}
	}
	j.lock.RUnlock()
}

func (j Job) String() string {
	return fmt.Sprintf("Payload: %v\nCurrent_Retries: %d\nStatus: %d", j.P, j.CurrentRetries, j.Status)
}




