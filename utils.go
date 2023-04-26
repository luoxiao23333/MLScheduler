package main

import (
	"strconv"
	"sync"
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
