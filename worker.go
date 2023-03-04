package main

import "fmt"

var WorkerPool []Worker

const workerPort = ":8080"

type Worker string

func (url Worker) GetURL(route string) string {
	return fmt.Sprintf("http://%v%v/%v", url, workerPort, route)
}

func (url Worker) GetIP() string {
	return string(url)
}

func AddWorker(hostName string) {
	WorkerPool = append(WorkerPool, Worker(hostName))
}
