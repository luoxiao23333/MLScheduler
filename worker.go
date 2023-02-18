package main

import "fmt"

var WorkerURLPool []workerURL

const workerPort = ":8080"

type workerURL string

func (url workerURL) GetURL(route string) string {
	return fmt.Sprintf("http://%v%v/%v", url, workerPort, route)
}

func (url workerURL) GetIP() string {
	return string(url)
}

func AddWorker(hostName string) {
	WorkerURLPool = append(WorkerURLPool, workerURL(hostName))
}
