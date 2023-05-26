package handler

import (
	"Scheduler/buffer_pool"
	"Scheduler/worker_pool"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
)

func doSlam(worker *worker_pool.Worker, form *multipart.Form, taskID string) {

	// submit slam task to the worker_pool
	//log.Printf("submit to %v", worker.GetIP())

	workerURL := worker.GetURL("run_task")

	bufferElem := buffer_pool.GetBuffer()
	postBody := bufferElem.Buffer

	multipartWriter := multipart.NewWriter(postBody)

	if err := multipartWriter.WriteField("task_name", "slam"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_id", taskID); err != nil {
		log.Panic(err)
	}

	fileHeader := form.File["frame"][0]
	writer, err := multipartWriter.CreateFormFile("frame", "input.png")
	if err != nil {
		log.Panic(err)
	}
	file, err := fileHeader.Open()
	if err != nil {
		log.Panic(err)
	}

	frameBytes, err := io.ReadAll(file)
	log.Printf("Receive bytes %v", len(frameBytes))
	if err != nil {
		log.Panic(err)
	}

	err = file.Close()
	if err != nil {
		log.Panic(err)
	}

	_, err = writer.Write(frameBytes)
	//log.Printf("Write %v bytes. Buffer size is %v", n, postBody.Cap())
	if err != nil {
		log.Panic(err)
	}

	if err = multipartWriter.WriteField("reset", "False"); err != nil {
		log.Panic(err)
	}

	err = multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	notifier := make(chan *multipart.Form)
	taskFinishNotifier.Store(taskID, notifier)

	_, err = http.Post(workerURL, multipartWriter.FormDataContentType(), postBody)
	if err != nil {
		log.Panic(err)
	}

	buffer_pool.ReturnBuffer(bufferElem)
}

func slamFinish(w http.ResponseWriter, r *http.Request) {
	multipartReader, err := r.MultipartReader()
	if err != nil {
		log.Panic(err)
	}

	form, err := multipartReader.ReadForm(15 * 1024 * 1024)
	if err != nil {
		log.Panic(err)
	}

	// If is reset request
	if len(form.Value["slam_result"]) == 0 {
		return
	}

	taskID := form.Value["task_id"][0]

	notifier, _ := taskFinishNotifier.Load(taskID)

	notifier.(chan *multipart.Form) <- form
}

func SendBackSlam(r *http.Request, taskID string, worker *worker_pool.Worker,
	returnWorker, deleteWorker bool) {
	go func(clientIP string) {
		notifier, _ := taskFinishNotifier.Load(taskID)
		finishForm := <-notifier.(chan *multipart.Form)
		close((notifier.(chan *multipart.Form)))
		taskFinishNotifier.Delete(taskID)

		if returnWorker {
			workerURL := worker.GetURL("run_task")

			resetBufferElem := buffer_pool.GetBuffer()
			postBody := resetBufferElem.Buffer
			multipartWriter := multipart.NewWriter(postBody)

			if err := multipartWriter.WriteField("reset", "True"); err != nil {
				log.Panic(err)
			}

			if err := multipartWriter.WriteField("task_name", "slam"); err != nil {
				log.Panic(err)
			}

			if err := multipartWriter.WriteField("task_id", taskID); err != nil {
				log.Panic(err)
			}

			err := multipartWriter.Close()
			if err != nil {
				log.Panic(err)
			}

			_, err = http.Post(workerURL, multipartWriter.FormDataContentType(), postBody)
			if err != nil {
				log.Panic(err)
			}

			buffer_pool.ReturnBuffer(resetBufferElem)

			worker.ReturnToPool(taskID)
		}

		if deleteWorker {
			worker.DeleteWorker()
			log.Printf("worker deleted")
		}

		log.Printf("receive result of task id: %v.task id is %v, slam_result is %v",
			taskID, len(finishForm.Value["task_id"]), len(finishForm.Value["slam_result"]))

		sendBackBufferElem := buffer_pool.GetBuffer()
		buffer := sendBackBufferElem.Buffer
		multipartWriter := multipart.NewWriter(buffer)

		if err := multipartWriter.WriteField("task_id",
			finishForm.Value["task_id"][0]); err != nil {
			log.Panic(err)
		}

		if err := multipartWriter.WriteField("slam_result",
			finishForm.Value["slam_result"][0]); err != nil {
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

		buffer_pool.ReturnBuffer(sendBackBufferElem)

	}(r.RemoteAddr)
}
