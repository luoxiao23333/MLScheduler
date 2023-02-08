package main

import (
	"log"
	"os"
)

const (
	LogFileName = "scheduler.log"
	PermUserRW  = 0644 // PermUserRW -rw-r--r--
)

func initLog() {
	logFile, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, PermUserRW)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)
}
