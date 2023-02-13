package main

import "log"

func main() {
	initLog()
	log.Println("Start Scheduler")
	RunHttpServer()
}
