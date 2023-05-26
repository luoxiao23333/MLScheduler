package main

import (
	"Scheduler/buffer_pool"
	"Scheduler/handler"
	"Scheduler/utils"
	"Scheduler/worker_pool"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"
)

var schedulerPort = ":8081"

const (
	STATUS_BEGIN   = "Begin"
	STATUS_RUNNING = "Running"
	STATUS_LAST    = "Last"
)

type router struct{}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	//log.Printf("Receive Path: [%v]", req.URL.Path)
	switch req.URL.Path {
	case "/new_task":
		newTask(w, req)
	case "/complete_task":
		completeTask(w, req)
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
	case "/det_finish":
		handler.GetHandler("det").FinishTask(w, req)
	case "/update_cpu":
		updateCPU(w, req)
	case "/create_workers":
		createWorkers(w, req)
	case "/restart":
		restart(w, req)
	case "/debug/pprof/profile":
		pprof.Profile(w, req)
	default:
		http.NotFound(w, req)
	}
}

func RunHttpServer() {
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
	TaskName      string         `json:"task_name"`
	GpuLimits     map[string]int `json:"gpu_limit"`
	GpuMemory     map[string]int `json:"gpu_memory"`
	BatchSize     map[string]int
}

func restart(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("OK"))
	r.Body.Close()
	os.Exit(0)
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
		"controller": 8,
		"as1":        4,
		"gpu1":       3,
	}

	utils.DebugWithTimeWait("Before creating workers")
	log.Printf("Creating some workers... \n%v", info)
	worker_pool.InitWorkers(info.WorkerNumbers, info.BatchSize, info.CpuLimits,
		info.GpuLimits, info.GpuMemory, info.TaskName)
	utils.DebugWithTimeWait("After creating workers")
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

type CompleteTaskInfo struct {
	DETNodeName        string `json:"det_node_name"`
	DETTaskID          string `json:"det_task_id"`
	FusionNodeName     string `json:"fusion_node_name"`
	FusionTaskID       string `json:"fusion_task_id"`
	Status             string `json:"status"`
	DeleteDETWorker    bool   `json:"delete_det_worker"`
	DeleteFusionWorker bool   `json:"delete_fusion_worker"`
}

func completeTask(w http.ResponseWriter, r *http.Request) {

	reader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := reader.ReadForm(1024 * 1024 * 2)
	if err != nil {
		log.Panic(err)
	}

	if len(form.Value["json"]) != 1 {
		log.Panicf("Expected len %v, got %v", 1, len(form.Value["json"]))
	}

	rawJson := form.Value["json"][0]
	taskInfo := &CompleteTaskInfo{}
	json.Unmarshal([]byte(rawJson), taskInfo)

	var detWorker, fusionWorker *worker_pool.Worker
	var detTaskID, fusionTaskID string

	if taskInfo.Status == STATUS_BEGIN {
		detTaskID = utils.GetUniqueID()
		fusionTaskID = utils.GetUniqueID()

		detWorker = worker_pool.OccupyWorker("det", detTaskID, taskInfo.DETNodeName)
		fusionWorker = worker_pool.OccupyWorker("fusion", fusionTaskID, taskInfo.FusionNodeName)
	} else {
		detWorker = worker_pool.GetWorkerByTaskID(taskInfo.DETTaskID)
		fusionWorker = worker_pool.GetWorkerByTaskID(taskInfo.FusionTaskID)

		if detWorker == nil || fusionWorker == nil {
			log.Printf("Maybe work not complete before delete, occationally internal bugs")
			w.Write([]byte("Failed"))
			return
		}

		detTaskID = taskInfo.DETTaskID
		fusionTaskID = taskInfo.FusionTaskID
	}

	taskHandler := handler.NewCompleteTaskHandler(
		detWorker,
		fusionWorker,
		detTaskID,
		fusionTaskID,
		form,
		taskInfo.Status,
		taskInfo.DeleteDETWorker,
		taskInfo.DeleteFusionWorker,
		r.RemoteAddr)
	go taskHandler.SendTask()

	_, err = w.Write([]byte(fmt.Sprintf("%v:%v", detTaskID, fusionTaskID)))
	if err != nil {
		log.Panic(err)
	}
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

	if status == STATUS_BEGIN {
		taskID = utils.GetUniqueID()
		// TODO Make Decision Here, Apply True Resource Allocation
		// Default Round Robin and Allocate Expected Resource
		worker = worker_pool.OccupyWorker(taskName, taskID, nodeName)
		//worker := worker_pool.CreateWorker(podsInfo.TaskName, podsInfo.NodeName, podsInfo.HostName, cpuLimit)
		//worker.bindTaskID(strconv.Itoa(taskID))
		returnWorker = false
	} else if status == STATUS_RUNNING {
		worker = worker_pool.GetWorkerByTaskID(taskID)
		returnWorker = false
	} else if status == STATUS_LAST {
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
	now := time.Now()
	handlers.StartTask(worker, form, taskID)
	log.Printf("New Task start task %v", time.Since(now))
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
