package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
)

var schedulerPort = ":8081"

func RunHttpServer() {
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("/worker_register", workerRegister)
	http.HandleFunc("/new_tracking", newTracking)

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

// Receive a task from devices, and submit to specific worker
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

	task := form.Value["task"][0]
	if err != nil {
		log.Panic(err)
	}

	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	worker := WorkerPool[taskID%len(WorkerPool)]

	if len(task) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	taskSubmissionInfo := &TaskSubmissionInfo{
		ID:   taskID,
		Task: task,
	}

	if task == "object_detection" {
		marshalInfo := objectDetection(taskSubmissionInfo, worker, form)
		_, err = w.Write(marshalInfo)
		if err != nil {
			log.Panic(err)
		}
	} else {
		_, err = w.Write([]byte("Unsupported Command!"))
		if err != nil {
			log.Panic(err)
		}
	}
}

func objectDetection(taskInfo *TaskSubmissionInfo, worker Worker, form *multipart.Form) (
	marshalInfo []byte) {

	// submit object detection task to the worker

	marshal, err := json.Marshal(taskInfo)
	if err != nil {
		log.Panic(err)
	}

	workerURL := worker.GetURL("run_task")
	log.Printf("submit to %v, with info: %v", workerURL, taskInfo)

	body := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(body)
	err = multipartWriter.WriteField("json", string(marshal))
	if err != nil {
		log.Panic(err)
	}

	fileHeader := form.File["video"][0]
	if err != nil {
		log.Panic(err)
	}
	writer, err := multipartWriter.CreateFormFile("video", fileHeader.Filename)
	if err != nil {
		log.Panic(err)
	}
	file, err := fileHeader.Open()
	_, err = io.Copy(writer, file)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Task will sent with id = ", taskInfo.ID)

	// TODO
	// Receive what from object detection worker
	rep, err := http.DefaultClient.Post(workerURL, multipartWriter.FormDataContentType(),
		body)
	if err != nil {
		log.Panic(err)
	}

	marshalInfo, err = json.Marshal(taskInfo)
	if err != nil {
		log.Panic(err)
	}

	return marshalInfo

}

// receive tacking request from object detection
func newTracking(w http.ResponseWriter, r *http.Request) {
	taskID := GetUniqueID()
	worker := WorkerPool[taskID%len(WorkerPool)]

	pythonInfo, err := io.ReadAll(r.Body)
	if err != nil {
		log.Panic(err)
	}

	// send tracking request
	// TODO
	// Now will deadlock. After send task to tracking, need non-blocked way to receive tracking info
	// Scheduler->Tracking, Then tracking wait for OD's images, but OD wait for scheduler's tracking url
	// scheduler now wait for tracker's return, which should not be
	taskInfo := TaskSubmissionInfo{
		ID:   taskID,
		Task: "tracking",
	}
	marshal, err := json.Marshal(taskInfo)
	if err != nil {
		log.Panic(err)
	}
	body := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(body)
	err = multipartWriter.WriteField("json", string(marshal))
	err = multipartWriter.WriteField("pythonInfo", string(pythonInfo))
	_, err = http.Post(worker.GetURL("run_task"), multipartWriter.FormDataContentType(), body)
	if err != nil {
		log.Panic(err)
	}

	// send back object detection worker tracker's url
	// object detection worker will send images to tracker
	_, err = w.Write([]byte(worker.GetURL("tracking_upload")))
	if err != nil {
		log.Panic(err)
	}

}
