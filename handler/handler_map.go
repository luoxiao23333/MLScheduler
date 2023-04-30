package handler

import (
	"Scheduler/worker_pool"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"sync"
)

var taskFinishNotifier sync.Map

type StartTask func(worker *worker_pool.Worker, form *multipart.Form, taskID string)
type FinishTask func(w http.ResponseWriter, r *http.Request)
type SendBackResult func(r *http.Request, taskID string, worker *worker_pool.Worker,
	returnWorker, deleteWorker bool)

type Handler struct {
	StartTask
	FinishTask
	SendBackResult
}

var handlerMap *map[string]Handler

func GetHandler(taskName string) Handler {
	if handlerMap == nil {
		handlerMap = &map[string]Handler{
			"mcmot": {
				StartTask:      doMCMOT,
				FinishTask:     MCMOTFinish,
				SendBackResult: SendBackMCMOT,
			},
			"slam": {
				StartTask:      doSlam,
				FinishTask:     slamFinish,
				SendBackResult: SendBackSlam,
			},
			"fusion": {
				StartTask:      doFusion,
				FinishTask:     fusionFinish,
				SendBackResult: SendBackFusion,
			},
			"det": {
				StartTask:      doDET,
				FinishTask:     detFinish,
				SendBackResult: SendBackDET,
			},
		}
	}

	return (*handlerMap)[taskName]
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
