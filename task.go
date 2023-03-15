package main

// ExitStatus return process info after the process exited or terminated
type ExitStatus struct {
	Signal int `json:"signal"`
	Code   int `json:"code"`
}

// ExitInfo contains all info when a process exit
// Output the std output of the process, which is redirected to the Output filed
type ExitInfo struct {
	ExitStatus `json:"exitStatus"`
	Output     string `json:"output"`
}

// TaskInfo
// Contains info of a task
// TaskID unique ID in this node for the task
// ExitInfo Exit code, signal and execution output of this task
// StdOutput the standard output of the task. Like print(xxx)
type TaskInfo struct {
	TaskID   int      `json:"taskID"`
	ExitInfo ExitInfo `json:"exitInfo"`
	WorkerIP *string  `json:"workerIP"`
}
