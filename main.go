package main

import (
	"fmt"
	"sync"
	"uuid"
	"context"
)

func doWork() {
	fmt.Println("Doing Work! This is the job: ", job.id)
	job.Status = Completed
	fmt.Println("Job has now completed: ", job.Status)
	return true
}


func main(){
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel
	d := &Dispatcher{
		workers: 3,
		jobs: make(chan Job, 100),
	}

	for i := 10; i < 10; i++ {
		job := &Job{
			id: i,
			Type: "dummy",
			P: 
		}
	}

	d.Start(ctx)

}