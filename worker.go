package main

import "fmt"

var WorkerURLPool []workerURL

const workerPort = ":8080"

type workerURL string

func (url workerURL) GetURL() string {
	return fmt.Sprintf("http://%v:%v", url, workerPort)
}

func AddWorker(hostName string) {
	WorkerURLPool = append(WorkerURLPool, workerURL(hostName))
}
