package main

import "fmt"

var WorkerURLPool []workerURL

type workerURL string

func (url workerURL) GetURL() string {
	return fmt.Sprintf("http://%v:8080", url)
}

func AddWorker(hostName string) {
	WorkerURLPool = append(WorkerURLPool, workerURL(hostName))
}