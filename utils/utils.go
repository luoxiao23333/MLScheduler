package utils

import (
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var uniqueID int = 0
var idLock sync.Mutex

func GetUniqueID() string {
	var id int
	idLock.Lock()
	uniqueID++
	id = uniqueID
	idLock.Unlock()
	return strconv.Itoa(id)
}

func DebugWithTimeWait(message string) {
	if os.Getenv("Debug") == "False" {
		return
	}

	_, callerFile, callerLine, ok := runtime.Caller(1)
	if !ok {
		log.Println("Impossible for recovery debug info!")
	}

	log.Printf("\n----------------\nDebug Message: \n%v\nIn file: [%v], line: [%v]\n----------------",
		message, callerFile, callerLine)

	time.Sleep(1 * time.Second)
}
