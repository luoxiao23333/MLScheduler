package main

var uniqueID int = 0

func GetUniqueID() int {
	uniqueID++
	return uniqueID
}
