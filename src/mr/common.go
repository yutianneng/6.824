package mr

import "encoding/json"

func Any2String(data interface{}) string {
	marshal, _ := json.Marshal(data)
	return string(marshal)
}

type TaskStatus int

const (
	TaskStatusFinished    = 1
	TaskStatusProcessing  = 2
	TaskStatusUnallocated = 3
)

type TaskType int

const (
	TasktypeMap    TaskType = 1
	TasktypeReduce TaskType = 2
	TasktypeWait   TaskType = 3
)

type Code int

const (
	CodeSuccess  = 0
	CodeFail     = -1
	CodeShutdown = 1
)

type MapReduceTask struct {
	ID                    int
	Type                  TaskType
	Status                int
	NReduceWorker         int
	TargetWorker          string
	SourceFilePath        string
	IntermediateFilePaths []string
	OutputFilePath        string
}
