package main

import (
	"Scheduler/buffer_pool"
	"Scheduler/handler"
	"Scheduler/worker_pool"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"
)

var schedulerPort = ":8081"

type router struct{}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("Receive Path: [%v]", req.URL.Path)
	switch req.URL.Path {
	case "/new_task":
		newTask(w, req)
	case "/worker_register":
		workerRegister(w, req)
	case "/query_metric":
		queryMetrics(w, req)
	case "/mcmot_finish":
		handler.GetHandler("mcmot").FinishTask(w, req)
	case "/slam_finish":
		handler.GetHandler("slam").FinishTask(w, req)
	case "/fusion_finish":
		handler.GetHandler("fusion").FinishTask(w, req)
	case "/update_cpu":
		updateCPU(w, req)
	case "/create_workers":
		createWorkers(w, req)
	case "/debug/pprof/profile":
		pprof.Profile(w, req)
	default:
		http.NotFound(w, req)
	}
}

func RunHttpServer() {
	/*
		http.HandleFunc("/new_task", newTask)
		http.HandleFunc("/worker_register", workerRegister)
		http.HandleFunc("/query_metric", queryMetrics)
		http.HandleFunc("/mcmot_finish", handler.GetHandler("mcmot").FinishTask)
		http.HandleFunc("/slam_finish", handler.GetHandler("slam").FinishTask)
		http.HandleFunc("/fusion_finish", handler.GetHandler("fusion").FinishTask)
		http.HandleFunc("/update_cpu", updateCPU)
		http.HandleFunc("/create_workers", createWorkers)

		err := http.ListenAndServe(schedulerPort, nil)
		if err != nil {
			log.Panic(err)
		}*/

	server := &http.Server{
		Addr:         schedulerPort,
		ReadTimeout:  1 * time.Minute,
		WriteTimeout: 1 * time.Minute,
		IdleTimeout:  1 * time.Minute,
		Handler:      &router{},
	}

	if err := server.ListenAndServe(); err != nil {
		log.Panicf("listen: %s\n", err)
	}

}

type CreateInfo struct {
	CpuLimits     map[string]int `json:"cpu_limit"`
	WorkerNumbers map[string]int `json:"worker_numbers"`
	BatchSize     map[string]int
}

func createWorkers(w http.ResponseWriter, r *http.Request) {
	rawInfo, err := io.ReadAll(r.Body)
	if err != nil {
		log.Panic(err)
	}

	info := &CreateInfo{}

	if err = json.Unmarshal(rawInfo, info); err != nil {
		log.Panic(err)
	}

	info.BatchSize = map[string]int{
		"controller": 1,
		"as1":        1,
	}
	log.Printf("Creating some workers... \n%v", info)
	worker_pool.InitWorkers(info.WorkerNumbers, info.BatchSize, info.CpuLimits)
}

func updateCPU(w http.ResponseWriter, r *http.Request) {
	rawInfo, err := io.ReadAll(r.Body)
	if err != nil {
		log.Panic(err)
	}

	splitInfo := strings.Split(string(rawInfo), ":")
	log.Printf("receive updated requirement: %q", splitInfo)
	nodeName := splitInfo[0]
	rawCPU := splitInfo[1]

	cpuLimit, err := strconv.Atoi(rawCPU)
	if err != nil {
		log.Panic(err)
	}

	workerPool := worker_pool.GetWorkerPool("fusion")

	log.Printf("Has %v workers", len(workerPool))

	for _, taskType := range []string{"fusion"} {
		pool := worker_pool.GetWorkerPool(taskType)
		for _, worker := range pool {
			if worker.GetNodeName() == nodeName {
				worker.UpdateResourceLimit(int64(cpuLimit))
			}
		}
	}
}

// Each worker_pool node should register their IP When join the cluster
// TODO worker_pool nodes should also register their resources info
// workerRegister return back assigned port for the worker_pool
func workerRegister(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.RemoteAddr, ":")[0]

	bufferElem := buffer_pool.GetBuffer()
	buffer := bufferElem.Buffer
	if _, err := io.Copy(buffer, r.Body); err != nil {
		log.Panic(err)
	}

	buffer_pool.ReturnBuffer(bufferElem)

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

	form, err := reader.ReadForm(1024 * 1024 * 2)
	if err != nil {
		log.Panic(err)
	}

	taskName := form.Value["task_name"][0]
	if err != nil {
		log.Panic(err)
	}

	nodeName := form.Value["node_name"][0]
	if err != nil {
		log.Panic(err)
	}

	status := form.Value["status"][0]
	if err != nil {
		log.Panic(err)
	}

	taskID := form.Value["task_id"][0]
	if err != nil {
		log.Panic(err)
	}

	var worker *worker_pool.Worker
	var returnWorker bool

	if status == "Begin" {
		taskID = GetUniqueID()
		// TODO Make Decision Here, Apply True Resource Allocation
		// Default Round Robin and Allocate Expected Resource
		podsInfo := worker_pool.PodsInfo[taskName+"-"+nodeName]
		worker = worker_pool.OccupyWorker(taskName, taskID, podsInfo.NodeName)
		//worker := worker_pool.CreateWorker(podsInfo.TaskName, podsInfo.NodeName, podsInfo.HostName, cpuLimit)
		//worker.bindTaskID(strconv.Itoa(taskID))
		returnWorker = false
	} else if status == "Running" {
		worker = worker_pool.GetWorkerByTaskID(taskID)
		returnWorker = false
	} else if status == "Last" {
		worker = worker_pool.GetWorkerByTaskID(taskID)
		returnWorker = true
	}

	log.Printf("Receive task %v, assigned id %v, worker_pool %v", taskName, taskID, worker.Describe())
	if len(taskName) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	deleteWorker := len(form.Value["delete"]) != 0

	handlers := handler.GetHandler(taskName)
	handlers.StartTask(worker, form, taskID)
	handlers.SendBackResult(r, taskID, worker, returnWorker, deleteWorker)

	_, err = w.Write([]byte(taskID))
	if err != nil {
		log.Panic(err)
	}
}

func queryMetrics(w http.ResponseWriter, r *http.Request) {
	bufferElem := buffer_pool.GetBuffer()
	buffer := bufferElem.Buffer
	if _, err := io.Copy(buffer, r.Body); err != nil {
		log.Panic(err)
	}

	taskID := buffer.String()
	buffer_pool.ReturnBuffer(bufferElem)
	worker := worker_pool.GetWorkerByTaskID(taskID)
	var usage *worker_pool.ResourceUsage
	if worker != nil {
		usage = worker_pool.QueryResourceUsage(worker.GetPodName())
	} else {
		usage = &worker_pool.ResourceUsage{
			CPU:              0,
			Memory:           0,
			Storage:          0,
			StorageEphemeral: 0,
			CollectedTime:    "Task has been ended",
			Window:           0,
			Available:        false,
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
