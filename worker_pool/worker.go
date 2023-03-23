package worker_pool

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)

var workerMap = make(map[string][]*Worker)

var workerSelectionLock = sync.Mutex{}

// map from ip to current assigned port
var portPoolMap = make(map[string]int)

type Worker struct {
	ip          string
	taskType    string
	port        string
	isAvailable bool
}

func (w *Worker) GetURL(route string) string {
	return fmt.Sprintf("http://%v:%v/%v", w.ip, w.port, route)
}

func (w *Worker) GetIP() string {
	return w.ip
}

func (w *Worker) Describe() string {
	return fmt.Sprintf("[IP: %v, Task Type: %v]", w.ip, w.taskType)
}

// AddWorker return assigned port for the worker_pool
func AddWorker(hostName, taskType string) string {

	_, ok := portPoolMap[hostName]
	if !ok {
		portPoolMap[hostName] = 9000
	} else {
		portPoolMap[hostName] += 1
	}

	port := strconv.Itoa(portPoolMap[hostName])
	workerSelectionLock.Lock()
	workerMap[taskType] = append(workerMap[taskType], &Worker{
		ip:          hostName,
		taskType:    taskType,
		port:        port,
		isAvailable: true,
	})
	workerSelectionLock.Unlock()

	return port
}

func GetWorker(taskType string) *Worker {
	workerPool := workerMap[taskType]
	if len(workerPool) == 0 {
		log.Panicf("task type %v has no worker_pool!", taskType)
	}

	// Do Worker Selection
	var chooseWorker *Worker = nil
	for {
		workerSelectionLock.Lock()
		for i := 0; i < len(workerPool); i++ {
			if workerPool[i].isAvailable {
				chooseWorker = workerPool[i]
				chooseWorker.isAvailable = false
			}
		}

		if chooseWorker == nil {
			log.Printf("Do not has support worker_pool for %v, has %v unavaliable workers, wait for 5 seconds",
				taskType, len(workerPool))
		} else {
			workerSelectionLock.Unlock()
			break
		}
		workerSelectionLock.Unlock()
		time.Sleep(5 * time.Second)
	}

	return chooseWorker
}

func (w *Worker) ReturnToPool() {
	workerSelectionLock.Lock()
	w.isAvailable = true
	workerSelectionLock.Unlock()
}
