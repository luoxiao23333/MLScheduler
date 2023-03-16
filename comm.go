package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

var schedulerPort = ":8081"

var taskFinishNotifier sync.Map

func RunHttpServer() {
	http.HandleFunc("/new_task", newTask)
	http.HandleFunc("/worker_register", workerRegister)
	http.HandleFunc("/object_detection_finish", objectDetectionFinish)

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

	taskName := form.Value["task_name"][0]
	if err != nil {
		log.Panic(err)
	}

	taskID := GetUniqueID()

	// TODO Make Decision Here, Apply True Resource Allocation
	// Default Round Robin and Allocate Expected Resource
	worker := WorkerPool[taskID%len(WorkerPool)]

	if len(taskName) == 0 {
		_, err = w.Write([]byte("Un Complete Params!"))
		if err != nil {
			log.Panic(err)
		}
	}

	if taskName == "object_detection" {
		notifier := make(chan *multipart.Form, 1)
		taskFinishNotifier.Store(taskID, notifier)

		log.Printf("submit to %v", worker.GetIP())
		objectDetection(worker, form, taskID)

		go func(clientIP string) {
			finishForm := <-notifier

			log.Printf("receive result of task id: %v", taskID)

			buffer := &bytes.Buffer{}
			multipartWriter := multipart.NewWriter(buffer)

			saveFile("video", "output.mp4", finishForm, multipartWriter)
			saveFile("bbox_txt", "output.txt", finishForm, multipartWriter)
			saveFile("bbox_xlsx", "output.xlsx", finishForm, multipartWriter)

			if err = multipartWriter.WriteField("container_output",
				finishForm.Value["container_output"][0]); err != nil {
				log.Panic(err)
			}

			err = multipartWriter.Close()
			if err != nil {
				log.Panic(err)
			}

			// split ip and port
			clientIP = "http://" + strings.Split(clientIP, ":")[0]

			log.Printf("result send back to %v", clientIP+":8080/object_detection")

			_, err = http.Post(clientIP+":8080/object_detection", multipartWriter.FormDataContentType(), buffer)
			if err != nil {
				log.Panic(err)
			}
		}(r.RemoteAddr)

		_, err = w.Write([]byte(fmt.Sprintf("Task has been submitted to %v", worker.GetIP())))
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

func objectDetection(worker Worker, form *multipart.Form, taskID int) {

	// submit object detection task to the worker

	workerURL := worker.GetURL("run_task")

	postBody := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(postBody)

	if err := multipartWriter.WriteField("task_name", "object_detection"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_id", strconv.Itoa(taskID)); err != nil {
		log.Panic(err)
	}

	fileHeader := form.File["video"][0]
	writer, err := multipartWriter.CreateFormFile("video", "input.avi")
	if err != nil {
		log.Panic(err)
	}
	file, err := fileHeader.Open()
	_, err = io.Copy(writer, file)
	if err != nil {
		log.Panic(err)
	}

	err = multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	_, err = http.Post(workerURL, multipartWriter.FormDataContentType(), postBody)
	if err != nil {
		log.Panic(err)
	}
}

func objectDetectionFinish(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(100 * 1024 * 1024)
	taskID, err := strconv.Atoi(form.Value["task_id"][0])
	if err != nil {
		log.Panic(err)
	}

	notifier, _ := taskFinishNotifier.LoadAndDelete(taskID)

	notifier.(chan *multipart.Form) <- form
}

func saveFile(fieldName, fileName string, form *multipart.Form, multipartWriter *multipart.Writer) {
	file, err := form.File[fieldName][0].Open()
	if err != nil {
		log.Panic(err)
	}

	formFile, err := multipartWriter.CreateFormFile(fieldName, fileName)
	if err != nil {
		log.Panic(err)
	}

	_, err = io.Copy(formFile, file)
	if err != nil {
		log.Panic(err)
	}

	err = file.Close()
	if err != nil {
		log.Panic(err)
	}

}
