package main

import (
	"Scheduler/handler"
	"Scheduler/worker_pool"
	"bytes"
	"io"
	"log"
	"net/http"
	"strings"
)

var schedulerPort = ":8081"

func RunHttpServer() {
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("/worker_register", workerRegister)
	http.HandleFunc("/mcmot_finish", handler.GetHandler("mcmot").FinishTask)
	http.HandleFunc("/slam_finish", handler.GetHandler("slam").FinishTask)

	err := http.ListenAndServe(schedulerPort, nil)
	if err != nil {
		log.Panic(err)
	}
}

// Each worker_pool node should register their IP When join the cluster
// TODO worker_pool nodes should also register their resources info
// workerRegister return back assigned port for the worker_pool
func workerRegister(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.RemoteAddr, ":")[0]
	var port string

	buffer := &bytes.Buffer{}
	if _, err := io.Copy(buffer, r.Body); err != nil {
		log.Panic(err)
	}

	taskName := buffer.String()

	port = worker_pool.AddWorker(ip, taskName)

	log.Println("Worker ", ip, " Has been Registered")

	_, err := w.Write([]byte(port))
	if err != nil {
		log.Panic(err)
	}
}

// Receive a task from devices, and submit to specific worker_pool
// TODO apply and plug Scheduling and Resource Allocation Strategy
func newTask(w http.ResponseWriter, r *http.Request) {
	reader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := reader.ReadForm(1024 * 1024 * 100)
	if err != nil {
		log.Panic(err)
	}

	taskName := form.Value["task_name"][0]
	if err != nil {
		log.Panic(err)
	}

	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	worker := worker_pool.GetWorker(taskName)

	log.Printf("Receive task %v, assigned id %v, worker_pool %v", taskName, taskID, worker.Describe())

	if len(taskName) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	handlers := handler.GetHandler(taskName)
	handlers.StartTask(worker, form, taskID)
	handlers.SendBackResult(w, r, taskID, worker, form)
}
