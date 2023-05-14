package handler

import (
	"Scheduler/buffer_pool"
	"Scheduler/worker_pool"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
)

const STATUS_LAST = "Last"

type CompleteTaskHandler struct {
	detWorker    *worker_pool.Worker
	fusionWorker *worker_pool.Worker

	detTaskID    string
	fusionTaskID string

	form   *multipart.Form
	status string

	deleteDETWorker    bool
	deleteFusionWorker bool

	clientAddress string
}

func NewCompleteTaskHandler(
	detWorker *worker_pool.Worker,
	fusionWorker *worker_pool.Worker,
	detTaskID string,
	fusionTaskID string,
	form *multipart.Form,
	status string,
	deleteDETWorker bool,
	deleteFusionWorker bool,
	clientAddress string) *CompleteTaskHandler {
	return &CompleteTaskHandler{
		detWorker:          detWorker,
		fusionWorker:       fusionWorker,
		detTaskID:          detTaskID,
		fusionTaskID:       fusionTaskID,
		form:               form,
		status:             status,
		deleteDETWorker:    deleteDETWorker,
		deleteFusionWorker: deleteFusionWorker,
		clientAddress:      clientAddress,
	}
}

func (handler *CompleteTaskHandler) SendTask() {
	wg := sync.WaitGroup{}
	wg.Add(2)
	var detResult string
	go func() {
		// start det and get det result
		detResult = handler.sendToDET()
		wg.Done()
	}()

	go func() {
		// trigger localization
		handler.startLocalization()
		wg.Done()
	}()

	wg.Wait()

	// send det result to the fusion, fusion worker will complete
	// localization first, then do fusion, then sendback result
	fusionResult := handler.sendDETResultToFusion(detResult)

	handler.sendBackToClient(fusionResult)
}

func (handler *CompleteTaskHandler) startLocalization() {
	log.Printf("submit to %v", handler.fusionWorker.GetIP())

	workerURL := handler.fusionWorker.GetURL("run_task")

	bufferElem := buffer_pool.GetBuffer()
	postBody := bufferElem.Buffer

	multipartWriter := multipart.NewWriter(postBody)

	if err := multipartWriter.WriteField("cmd", "slam"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_name", "fusion"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_id", handler.fusionTaskID); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("reset", "False"); err != nil {
		log.Panic(err)
	}

	fileHeader := handler.form.File["frame"][0]
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

	n, err := writer.Write(frameBytes)
	log.Printf("Write %v bytes. Buffer size is %v", n, postBody.Cap())
	if err != nil {
		log.Panic(err)
	}

	err = file.Close()
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

	buffer_pool.ReturnBuffer(bufferElem)
}

func (handler *CompleteTaskHandler) sendToDET() string {
	detHandler := GetHandler("det")
	detHandler.StartTask(handler.detWorker, handler.form, handler.detTaskID)

	notifier, _ := taskFinishNotifier.Load(handler.detTaskID)
	finishForm := <-notifier.(chan *multipart.Form)
	close((notifier.(chan *multipart.Form)))
	taskFinishNotifier.Delete(handler.detTaskID)

	if handler.status == STATUS_LAST {
		workerURL := handler.detWorker.GetURL("run_task")

		resetBufferElem := buffer_pool.GetBuffer()
		postBody := resetBufferElem.Buffer
		multipartWriter := multipart.NewWriter(postBody)

		if err := multipartWriter.WriteField("reset", "True"); err != nil {
			log.Panic(err)
		}

		if err := multipartWriter.WriteField("task_name", "det"); err != nil {
			log.Panic(err)
		}

		if err := multipartWriter.WriteField("task_id", handler.detTaskID); err != nil {
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

		handler.detWorker.ReturnToPool(handler.detTaskID)
	}

	if handler.deleteDETWorker {
		handler.detWorker.DeleteWorker()
		log.Printf("det worker deleted")
	}

	log.Printf("receive result of task id: %v.task id is %v, det_result is %v",
		handler.detTaskID, len(finishForm.Value["task_id"]), len(finishForm.Value["det_result"]))

	return finishForm.Value["det_result"][0]
}

func (handler *CompleteTaskHandler) sendDETResultToFusion(detResult string) string {
	// submit fusion task to the worker_pool
	log.Printf("submit to %v", handler.fusionWorker.GetIP())

	workerURL := handler.fusionWorker.GetURL("run_task")

	bufferElem := buffer_pool.GetBuffer()
	postBody := bufferElem.Buffer

	multipartWriter := multipart.NewWriter(postBody)

	if err := multipartWriter.WriteField("cmd", "fusion"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_name", "fusion"); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("task_id", handler.fusionTaskID); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("detect_result", detResult); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("reset", "False"); err != nil {
		log.Panic(err)
	}

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	notifier := make(chan *multipart.Form)
	taskFinishNotifier.Store(handler.fusionTaskID, notifier)

	_, err = http.Post(workerURL, multipartWriter.FormDataContentType(), postBody)
	if err != nil {
		log.Panic(err)
	}

	buffer_pool.ReturnBuffer(bufferElem)

	finishForm := <-notifier
	if len(finishForm.Value["fusion_result"]) != 1 {
		log.Panicf("len of fusion result is %v", len(finishForm.Value["fusion_result"]))
	}

	if handler.status == STATUS_LAST {
		resetBufferElem := buffer_pool.GetBuffer()
		postBody := resetBufferElem.Buffer
		multipartWriter := multipart.NewWriter(postBody)

		if err := multipartWriter.WriteField("reset", "True"); err != nil {
			log.Panic(err)
		}

		if err := multipartWriter.WriteField("task_name", "fusion"); err != nil {
			log.Panic(err)
		}

		if err := multipartWriter.WriteField("task_id", handler.fusionTaskID); err != nil {
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

		handler.fusionWorker.ReturnToPool(handler.fusionTaskID)
	}

	if handler.deleteFusionWorker {
		handler.fusionWorker.DeleteWorker()
		log.Printf("fusion worker deleted")
	}

	return finishForm.Value["fusion_result"][0]
}

func (handler *CompleteTaskHandler) sendBackToClient(fusionResult string) {

	sendBackBufferElem := buffer_pool.GetBuffer()
	buffer := sendBackBufferElem.Buffer
	multipartWriter := multipart.NewWriter(buffer)

	if err := multipartWriter.WriteField("det_task_id", handler.detTaskID); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("fusion_task_id", handler.fusionTaskID); err != nil {
		log.Panic(err)
	}

	if err := multipartWriter.WriteField("fusion_result", fusionResult); err != nil {
		log.Panic(err)
	}

	err := multipartWriter.Close()
	if err != nil {
		log.Panic(err)
	}

	// split ip and port
	clientIP := "http://" + strings.Split(handler.clientAddress, ":")[0]
	resultAddress := clientIP + ":8080/complete_task"

	log.Printf("result send back to %v", resultAddress)

	_, err = http.Post(resultAddress, multipartWriter.FormDataContentType(), buffer)
	if err != nil {
		log.Panic(err)
	}

	buffer_pool.ReturnBuffer(sendBackBufferElem)
}
