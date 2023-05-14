package worker_pool

import (
	"Scheduler/utils"
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// var WorkerMap = make(map[string]map[string]*Worker)
// map[task type]map[unque pod name]*Worker
var WorkerMap sync.Map

var workerSelectionLock = sync.Mutex{}

// map from ip to current assigned port
var portPoolMap = make(map[string]int)

var taskIDWorkerMap sync.Map

type Worker struct {
	ip          string
	taskType    string
	port        string
	isAvailable bool
	podName     string
	taskID      string
	nodeName    string
	wokerName   string
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

// addWorker return assigned port for the worker_pool
var portAssignLock = sync.Mutex{}

func addWorker(hostName, taskType, nodeName string) *Worker {
	portAssignLock.Lock()
	_, ok := portPoolMap[hostName]
	if !ok {
		portPoolMap[hostName] = 20000
	} else {
		portPoolMap[hostName] += 1
	}

	port := strconv.Itoa(portPoolMap[hostName])
	portAssignLock.Unlock()

	workerSelectionLock.Lock()

	newWorker := &Worker{
		ip:          hostName,
		taskType:    taskType,
		port:        port,
		isAvailable: true,
		nodeName:    nodeName,
		wokerName:   fmt.Sprintf("%v-%v-%v", taskType, port, nodeName),
	}

	// map from task type to workerMap
	// then map from workerName to specific Worker
	workerPool, _ := WorkerMap.LoadOrStore(taskType, &map[string]*Worker{})

	workerMap, _ := workerPool.(*map[string]*Worker)
	(*workerMap)[newWorker.wokerName] = newWorker

	workerSelectionLock.Unlock()

	return newWorker
}

func (w *Worker) GetWorkerName() string {
	return w.wokerName
}

func OccupyWorker(taskType, taskID, nodeName string) *Worker {
	nodeName = PodsInfo[taskType+"-"+nodeName].NodeName

	rawPool, _ := WorkerMap.Load(taskType)
	workerPool := rawPool.(*map[string]*Worker)

	if len(*workerPool) == 0 {
		log.Panicf("task type %v has no worker_pool!", taskType)
	}

	// Do Worker Selection
	var chooseWorker *Worker = nil
	for {
		workerSelectionLock.Lock()
		for podName, worker := range *workerPool {
			if worker.isAvailable && worker.nodeName == nodeName {
				chooseWorker = worker
				(*workerPool)[podName].isAvailable = false
				chooseWorker.bindTaskID(taskID)
				break
			}
		}

		if chooseWorker == nil {
			log.Printf("Do not has support worker_pool for %v, has %v unavaliable workers, wait for 50 msec",
				taskType, len(*workerPool))
		} else {
			workerSelectionLock.Unlock()
			break
		}
		workerSelectionLock.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	return chooseWorker
}

func (w *Worker) ReturnToPool(taskID string) {
	workerSelectionLock.Lock()
	w.isAvailable = true
	taskIDWorkerMap.Delete(taskID)
	workerSelectionLock.Unlock()
}

func (w *Worker) GetPodName() string {
	return w.podName
}

func (w *Worker) DeleteWorker() {
	workerSelectionLock.Lock()
	if !w.isAvailable {
		workerSelectionLock.Unlock()
		log.Panicf("Delete worker %v before return", w.nodeName)
	}
	rawPool, _ := WorkerMap.Load(w.taskType)
	workerPool := rawPool.(*map[string]*Worker)
	delete(*workerPool, w.wokerName)
	workerSelectionLock.Unlock()
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Panic(err)
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Panic(err)
	}

	podsClient := clientSet.CoreV1().Pods("default")
	err = podsClient.Delete(context.Background(), w.podName, metav1.DeleteOptions{})
	if err != nil {
		log.Panic(err)
	}

	// wait until pod deleted
	err = wait.PollImmediate(500*time.Millisecond, 2*time.Minute, func() (bool, error) {
		_, err = clientSet.CoreV1().Pods("default").Get(context.Background(),
			w.GetPodName(), metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return true, nil
		} else if err != nil {
			log.Panic(err)
		}
		return false, nil
	})
	if err != nil {
		log.Panic(err)
	}

	taskIDWorkerMap.Delete(w.taskID)

	log.Printf("Pod %v deleted", w.podName)
}

func (w *Worker) bindTaskID(taskID string) {
	taskIDWorkerMap.Store(taskID, w)
	w.taskID = taskID
}

func GetWorkerByTaskID(taskID string) *Worker {
	worker, ok := taskIDWorkerMap.Load(taskID)
	if ok {
		return worker.(*Worker)
	} else {
		log.Panic("worker with taskID %v does not exist!!!", taskID)
		return nil
	}
}

func InitWorkers(workerNumbers, batchSizes, cpuLimits, gpuLimits, gpuMemorys map[string]int,
	taskName string) []*Worker {
	var pool []*Worker
	wg := sync.WaitGroup{}
	wg.Add(len(workerNumbers))
	for nodeName, workerNumber := range workerNumbers {
		go func(nodeName string, workerNumber int) {
			for i := 0; i < workerNumber; i++ {
				podsInfo, ok := PodsInfo[taskName+"-"+nodeName]
				if !ok {
					log.Panicf("Unsupport combination %v", taskName+"-"+nodeName)
				}
				utils.DebugWithTimeWait(fmt.Sprintf("podinfo:[%v]", podsInfo))
				memLimit := "0"
				var cpuLimit string
				if cpuLimits[nodeName] != 0 {
					cpuLimit = fmt.Sprintf("%vm", cpuLimits[nodeName])
				} else {
					cpuLimit = "0"
				}
				gpuMemory := strconv.Itoa(gpuMemorys[nodeName])
				gpuLimit := strconv.Itoa(gpuLimits[nodeName])
				utils.DebugWithTimeWait("Before CreateWorker")
				worker := CreateWorker(podsInfo.TaskName, podsInfo.NodeName, podsInfo.HostName,
					cpuLimit, memLimit, gpuLimit, gpuMemory)
				utils.DebugWithTimeWait("After CreateWorker")
				pool = append(pool, worker)
				// slow down, too many slam init may make system down
				if (i+1)%batchSizes[nodeName] == 0 {
					log.Printf("Crated %v pods for %v", i+1, nodeName)
					time.Sleep(30 * time.Second)
				}
			}
			wg.Done()
		}(nodeName, workerNumber)
	}
	wg.Wait()
	return pool
}

func GetWorkerPool(taskType string) []*Worker {
	rawPool, _ := WorkerMap.Load(taskType)
	workerPool := rawPool.(map[string]*Worker)
	var pool []*Worker
	for _, worker := range workerPool {
		pool = append(pool, worker)
	}
	return pool
}

func (w *Worker) GetNodeName() string {
	return w.nodeName
}
