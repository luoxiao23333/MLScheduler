package main

import "Scheduler/worker_pool"

type NewPod struct {
	TaskName string
	NodeName string
	HostName string
}

var podsInfo = []NewPod{
	{
		TaskName: "slam",
		NodeName: "k8s-as1",
		HostName: "192.168.1.100",
	},
	{
		TaskName: "slam",
		NodeName: "k8s-as2",
		HostName: "192.168.1.103",
	},
	{
		TaskName: "mcmot",
		NodeName: "controller",
		HostName: "192.168.1.101",
	},
	{
		TaskName: "slam",
		NodeName: "controller",
		HostName: "192.168.1.101",
	},
}

func main() {
	initLog()

	for _, podInfo := range podsInfo {
		worker_pool.CreatePod(podInfo.TaskName, podInfo.NodeName, podInfo.HostName)
	}

	RunHttpServer()
}
