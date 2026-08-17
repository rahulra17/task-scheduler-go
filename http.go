package main

import(
	"fmt"
	"net/http"
	"encoding/json"
	"time"
	"strconv"
)

var id int = 0

// Create reciever to access appropriate data members from Dispatcher

// POST Method to create job
func (d *Dispatcher) CreateJobHandler(w http.ResponseWriter, r *http.Request){
	fmt.Println("Creating Job...")
	
	var req JobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)

	job := &Job{
		Id: id,
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

// GET method to read job
func (d *Dispatcher) GetJobHandler(w http.ResponseWriter, r *http.Request){
	id, err := strconv.Atoi(r.PathValue("id"))

	if err != nil{
		http.Error(w, "Bad Request boiiii", http.StatusBadRequest)
		return
	}

	job, found := d.js.ReadJob(id)

	if found{
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(job)
	} else{
		http.Error(w, "Job not found", http.StatusNotFound)
	}
}

// GET Method to read metrics
func (d *Dispatcher) GetMetricsHandler(w http.ResponseWriter, r *http.Request){
	metrics, err := d.m.GetMetrics()

	if err != nil{
		http.Error(w, "Server couldn't retrieve that for you", 500)
		return
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(metrics)
	}
}