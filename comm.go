package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

var schedulerURL = "http://192.168.0.101:8081"
var objectDetectionCMD = "python test.py"

var taskCompletedNotifier map[int]chan TaskInfo

func RunHttpServer() {
	http.HandleFunc("/task_end", taskEnd)
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("worker_register", workerRegister)

	err := http.ListenAndServe(schedulerURL, nil)
	if err != nil {
		log.Panic(err)
	}
}

func workerRegister(w http.ResponseWriter, r *http.Request) {
	AddWorker(r.Host)
	log.Println("Worker ", r.Host, " Has been Registered")

	_, err := w.Write([]byte("Registered Done!"))
	if err != nil {
		log.Panic(err)
	}
}

func taskEnd(w http.ResponseWriter, r *http.Request) {
	taskInfo := &TaskInfo{}
	var rawData []byte
	_, err := r.Body.Read(rawData)
	if err != nil {
		log.Panic("Http Read Failed")
	}

	err = json.Unmarshal(rawData, taskInfo)
	if err != nil {
		log.Panic("Json Unmarshal Failed")
	}

	taskCompletedNotifier[taskInfo.TaskID] <- *taskInfo
	_, err = w.Write([]byte(fmt.Sprint("Receive! Scheduler known task ", taskInfo.TaskID, " Done!")))
	if err != nil {
		log.Panic("Http Write Failed")
	}
}

func newTask(w http.ResponseWriter, r *http.Request) {
	param := r.URL.Query()
	task := param.Get("task")
	// Expected or Minimum Resource Requirement
	cpu := param.Get("cpu")
	mem := param.Get("mem")
	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	workerURL := WorkerURLPool[taskID % len(WorkerURLPool)].GetURL()

	if len(task) == 0 || len(cpu) == 0 || len(mem) == 0 {
		_, err := w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	if task == "object_detection" {
		urlString := fmt.Sprintf("%v?command=%v&cpu=%v&mem=%v&id=%v",
			workerURL, objectDetectionCMD, cpu, mem, taskID)

		log.Println("Execute: ", urlString)

		rep, err := http.DefaultClient.Get(urlString)
		if err != nil {
			log.Panic(err)
		}

		var rawData []byte
		_, err = rep.Body.Read(rawData)
		if err != nil {
			log.Panic(err)
		}

		taskID, err = strconv.Atoi(string(rawData))
		if err != nil {
			log.Panic(err)
		}

		log.Println("Task has been sent with id = ", taskID)

		taskCompletedNotifier[taskID] = make(chan TaskInfo, 1)
		// wait until @taskEnd has been invoked with certain taskID
		select {
		case taskInfo := <-taskCompletedNotifier[taskID]:
			log.Println("Task ID ", taskID, " ended. With Task info: ", taskInfo)
		}
	}

	_, err := w.Write([]byte("Unsupported Command!"))
	if err != nil {
		log.Panic(err)
	}

}
