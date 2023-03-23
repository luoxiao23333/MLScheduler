package main

import (
	"Scheduler/handler"
	"Scheduler/worker_pool"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var schedulerPort = ":8081"

func RunHttpServer() {
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("/worker_register", workerRegister)
	http.HandleFunc("/query_metric", queryMetrics)
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

	buffer := &bytes.Buffer{}
	if _, err := io.Copy(buffer, r.Body); err != nil {
		log.Panic(err)
	}

	log.Println("Worker ", ip, " Has been Registered")
}

// Receive a task from devices, and submit to specific worker_pool
// Write back task id
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
	worker := worker_pool.GetWorker(taskName, strconv.Itoa(taskID))

	log.Printf("Receive task %v, assigned id %v, worker_pool %v", taskName, taskID, worker.Describe())

	if len(taskName) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	handlers := handler.GetHandler(taskName)
	handlers.StartTask(worker, form, taskID)
	handlers.SendBackResult(r, taskID, worker)

	_, err = w.Write([]byte(strconv.Itoa(taskID)))
	if err != nil {
		log.Panic(err)
	}
}

func queryMetrics(w http.ResponseWriter, r *http.Request) {
	buffer := &bytes.Buffer{}
	if _, err := io.Copy(buffer, r.Body); err != nil {
		log.Panic(err)
	}

	taskID := buffer.String()
	worker := worker_pool.GetWorkerByTaskID(taskID)
	var usage worker_pool.ResourceUsage
	if worker != nil {
		usage = worker_pool.QueryResourceUsage(worker.GetPodName())
	} else {
		usage = worker_pool.ResourceUsage{
			CPU:              0,
			Memory:           0,
			Storage:          0,
			StorageEphemeral: 0,
			CollectedTime:    "Task has been ended",
			Window:           0,
		}
	}

	marshal, err := json.Marshal(usage)
	if err != nil {
		log.Panic(err)
	}
	_, err = w.Write(marshal)
	if err != nil {
		log.Panic(err)
	}

}
