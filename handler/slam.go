package handler

import (
	"Scheduler/worker_pool"
	"bytes"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
)

func doSlam(worker *worker_pool.Worker, form *multipart.Form, taskID int) {

	// submit slam task to the worker_pool
	log.Printf("submit to %v", worker.GetIP())

	workerURL := worker.GetURL("run_task")

	postBody := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(postBody)

	if err := multipartWriter.WriteField("task_name", "slam"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_id", strconv.Itoa(taskID)); err != nil {
		log.Panic(err)
	}

	fileHeader := form.File["video"][0]
	writer, err := multipartWriter.CreateFormFile("video", "input.mp4")
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

func slamFinish(w http.ResponseWriter, r *http.Request) {
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

func SendBackSlam(r *http.Request, taskID int, worker *worker_pool.Worker) {
	notifier := make(chan *multipart.Form, 1)
	taskFinishNotifier.Store(taskID, notifier)

	go func(clientIP string) {
		finishForm := <-notifier

		log.Printf("receive result of task id: %v", taskID)
		worker.ReturnToPool(strconv.Itoa(taskID))

		buffer := &bytes.Buffer{}
		multipartWriter := multipart.NewWriter(buffer)

		saveFile("key_frames", "KeyFrameTrajectory.txt", finishForm, multipartWriter)

		if err := multipartWriter.WriteField("container_output",
			finishForm.Value["container_output"][0]); err != nil {
			log.Panic(err)
		}

		err := multipartWriter.Close()
		if err != nil {
			log.Panic(err)
		}

		// split ip and port
		clientIP = "http://" + strings.Split(clientIP, ":")[0]

		log.Printf("result send back to %v", clientIP+":8080/slam")

		_, err = http.Post(clientIP+":8080/slam", multipartWriter.FormDataContentType(), buffer)
		if err != nil {
			log.Panic(err)
		}
	}(r.RemoteAddr)
}
