package main

import(
	"fmt"
	"net/http"
	"encoding/json"
	"time"
)

var id int = 0

func CreateJobHandler(w http.ResponseWriter, r *http.Request, d *Dispatcher){
	fmt.Println("Creating Job...")
	
	var req JobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Payload recieved successfully"))

	job := &Job{
		id: id,
		Type: req.Type,
		P: req.P,
		Status: Pending,
		MaxRetries: req.MaxRetries,
		CurrentRetries: 0,
		CreatedAt: time.Now(),
	}

	d.js.Save(job)
	d.Enqueue(job)

}

func GetJobHandler(w http.ResponseWriter, r *http.Request, d *Dispatcher){
	
}