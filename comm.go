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
var objectDetectionCMD = "python test.py"

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

func workerRegister(w http.ResponseWriter, r *http.Request) {
	ip := strings.Split(r.Host, ":")[0]
	AddWorker(ip)
	log.Println("Worker ", ip, " Has been Registered")

	_, err := w.Write([]byte("Registered Done!"))
	if err != nil {
		log.Panic(err)
	}
}

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

func newTask(w http.ResponseWriter, r *http.Request) {
	param := r.URL.Query()
	task := param.Get("task")
	// Expected or Minimum Resource Requirement
	cpu, err := strconv.Atoi(param.Get("cpu"))
	mem, err := strconv.Atoi(param.Get("mem"))
	if err != nil {
		log.Panic(err)
	}

	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	workerURL := WorkerURLPool[taskID%len(WorkerURLPool)].GetURL("run_task")

	if len(task) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	if task == "object_detection" {
		taskSubmissionInfo := &TaskSubmissionInfo{
			CPU:     cpu,
			Mem:     mem,
			Command: objectDetectionCMD,
			ID:      taskID,
		}

		marshal, err := json.Marshal(taskSubmissionInfo)
		if err != nil {
			log.Panic(err)
		}

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
