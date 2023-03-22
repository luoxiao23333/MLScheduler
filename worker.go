package main

import (
	"fmt"
	"log"
	"strconv"
)

var workerMap = make(map[string][]*Worker)

// map from ip to current assigned port
var portPoolMap = make(map[string]int)

type Worker struct {
	ip       string
	taskType string
	port     string
}

func (w Worker) GetURL(route string) string {
	return fmt.Sprintf("http://%v:%v/%v", w.ip, w.port, route)
}

func (w Worker) GetIP() string {
	return w.ip
}

func (w Worker) Describe() string {
	return fmt.Sprintf("[IP: %v, Task Type: %v]", w.ip, w.taskType)
}

// AddWorker return assigned port for the worker
func AddWorker(hostName, taskType string) string {

	_, ok := portPoolMap[hostName]
	if !ok {
		portPoolMap[hostName] = 9000
	} else {
		portPoolMap[hostName] += 1
	}

	port := strconv.Itoa(portPoolMap[hostName])
	workerMap[taskType] = append(workerMap[taskType], &Worker{
		ip:       hostName,
		taskType: taskType,
		port:     port,
	})

	return port
}

func GetWorker(taskType string) *Worker {
	workerPool := workerMap[taskType]
	if len(workerPool) == 0 {
		log.Panicf("task type %v has no worker!", taskType)
	}

	return workerPool[0]
}
