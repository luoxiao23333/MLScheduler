package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

var schedulerPort = ":8081"
var objectDetectionCMD = "python3 test.py"

// type: map[int]chan TaskInfo
var taskCompletedNotifier sync.Map

func RunHttpServer() {
	http.HandleFunc("/task_end", taskEnd)
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("/worker_register", workerRegister)

	err := http.ListenAndServe(schedulerPort, nil)
	if err != nil {
		log.Panic(err)
	}
}

// Each worker node should register their IP When join the cluster
// TODO worker nodes should also register their resources info
func workerRegister(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.RemoteAddr, ":")[0]
	AddWorker(ip)
	log.Println("Worker ", ip, " Has been Registered")

	_, err := w.Write([]byte("Registered Done!"))
	if err != nil {
		log.Panic(err)
	}
}

// When a task is finished or terminated, the worker node should notify the scheduler
// Notified Info: TaskInfo
func taskEnd(w http.ResponseWriter, r *http.Request) {
	taskInfo := &TaskInfo{}
	rawData, err := io.ReadAll(r.Body)
	if err != nil {
		log.Panicf("Http Read Failed: %v", err)
	}

	err = json.Unmarshal(rawData, taskInfo)
	if err != nil {
		log.Panicf("Json Unmarshal Failed: %v\nraw data: %v", err, string(rawData))
	}

	taskChan, _ := taskCompletedNotifier.Load(taskInfo.TaskID)
	taskChan.(chan TaskInfo) <- *taskInfo

	_, err = w.Write([]byte(fmt.Sprint("Receive! Scheduler known task ", taskInfo.TaskID, " Done!")))
	if err != nil {
		log.Panicf("Http Write Failed: %v", err)
	}
}

// Receive a task from devices, and submit to specific worker
// TODO apply and plug Scheduling and Resource Allocation Strategy
func newTask(w http.ResponseWriter, r *http.Request) {
	param := r.URL.Query()
	task := param.Get("task")
	// Expected or Minimum Resource Requirement
	_, err := strconv.Atoi(param.Get("cpu"))
	_, err = strconv.Atoi(param.Get("mem"))
	if err != nil {
		log.Panic(err)
	}

	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	worker := WorkerURLPool[taskID%len(WorkerURLPool)]

	if len(task) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	if task == "object_detection" {
		taskSubmissionInfo := &TaskSubmissionInfo{
			Command: objectDetectionCMD,
			ID:      taskID,
		}

		marshal, err := json.Marshal(taskSubmissionInfo)
		if err != nil {
			log.Panic(err)
		}

		workerURL :=  worker.GetURL("run_task")
		log.Printf("submit to %v, with info: %v", workerURL, taskSubmissionInfo)

		rep, err := http.DefaultClient.Post(workerURL, "application/json",
			bytes.NewReader(marshal))
		if err != nil {
			log.Panic(err)
		}

		rawData, err := io.ReadAll(rep.Body)
		if err != nil {
			log.Panic(err)
		}

		taskID, err = strconv.Atoi(string(rawData))
		if err != nil {
			log.Panic(err)
		}

		log.Println("Task has been sent with id = ", taskID)

		taskChan := make(chan TaskInfo, 1)
		taskCompletedNotifier.Store(taskID, taskChan)
		taskInfo := TaskInfo{}

		// wait until @taskEnd has been invoked with certain taskID
		select {
		case taskInfo, _ = <-taskChan:
			log.Println("Task ID ", taskID, " ended. With Task info: ", taskInfo)
		}

		taskInfo.WorkerIP = new(string)
		*taskInfo.WorkerIP = worker.GetIP()

		marshalInfo, err := json.Marshal(taskInfo)
		if err != nil {
			log.Panic(err)
		}
		_, err = w.Write(marshalInfo)
		if err != nil {
			log.Panic(err)
		}
		return
	}

	_, err = w.Write([]byte("Unsupported Command!"))
	if err != nil {
		log.Panic(err)
	}

}
