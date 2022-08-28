package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	m             *sync.Mutex
	NReduceWorker int
	MapTask       []*MapReduceTask
	ReduceTask    []*MapReduceTask

	MROutFile             string
	IntermediateFilePaths []string
	State                 chan bool
	IDGeranator           int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//resp不要重新分配对象，而是要对原来的对象进行赋值
func (c *Coordinator) GetTask(req *GetTaskArgs, resp *GetTaskReply) error {
	defer func() {
		log.Printf("master accept GetTask, req: %v, resp: %v", Any2String(req), Any2String(resp))
	}()
	c.m.Lock()
	defer c.m.Unlock()
	resp.Code = CodeSuccess
	for i := 0; i < len(c.MapTask); i++ {
		if c.MapTask[i].Status == TaskStatusUnallocated {
			resp.Task = &MapReduceTask{
				ID:             c.MapTask[i].ID,
				Type:           TasktypeMap,
				NReduceWorker:  c.NReduceWorker,
				SourceFilePath: c.MapTask[i].SourceFilePath,
				Status:         TaskStatusUnallocated,
			}

			c.MapTask[i].Status = TaskStatusProcessing
			c.MapTask[i].TargetWorker = req.Addr
			return nil
		}
	}
	for i := 0; i < len(c.ReduceTask); i++ {
		if c.ReduceTask[i].Status == TaskStatusUnallocated {
			resp.Task = &MapReduceTask{
				ID:                    c.ReduceTask[i].ID,
				Type:                  TasktypeReduce,
				NReduceWorker:         c.NReduceWorker,
				Status:                TaskStatusUnallocated,
				IntermediateFilePaths: c.ReduceTask[i].IntermediateFilePaths,
				OutputFilePath:        c.ReduceTask[i].OutputFilePath,
			}
			c.ReduceTask[i].Status = TaskStatusProcessing
			c.MapTask[i].TargetWorker = req.Addr
		}
	}
	log.Printf("no task to allocate!")
	return nil
}

func (c *Coordinator) UpdateTask(req *UpdateTaskArgs, resp *UpdateTaskReply) error {

	log.Printf("master accept UpdateTask: req: %v, resp: %v", Any2String(req), Any2String(resp))
	resp = &UpdateTaskReply{}
	task := req.Task
	switch task.Type {
	case TasktypeMap:
		if task.SourceFilePath != "" {
			for i := 0; i < len(c.MapTask); i++ {
				if c.MapTask[i].ID == task.ID {
					c.MapTask[i].Status = TaskStatusFinished
					//map worker写入的位置
					c.MapTask[i].IntermediateFilePaths = task.IntermediateFilePaths
					break
				}
			}
		}
	case TasktypeReduce:
		for i := 0; i < len(c.ReduceTask); i++ {
			if c.ReduceTask[i].ID == task.ID {
				c.ReduceTask[i].Status = TaskStatusFinished
				break
			}
		}
	default:
		fmt.Println("req type is matched!")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	//master阻塞等待结束

	return <-c.State
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		m:             &sync.Mutex{},
		NReduceWorker: nReduce,
		MapTask:       nil,
		ReduceTask:    nil,
		State:         make(chan bool),
	}
	tasks := make([]*MapReduceTask, 0, len(files))
	//从哪读，写到哪
	for i := 0; i < len(files); i++ {
		mt := &MapReduceTask{
			ID:             c.IDGeranator,
			Type:           TasktypeMap,
			NReduceWorker:  nReduce,
			SourceFilePath: files[i],
			Status:         TaskStatusUnallocated,
		}
		c.IDGeranator++
		tasks = append(tasks, mt)
	}
	c.MapTask = tasks
	//reduce task
	//从哪读，输出到哪
	//接收重点信号
	go func() {
		for true {
			flag := true
			for i := 0; i < len(c.MapTask); i++ {
				if c.MapTask[i].Status != TaskStatusFinished {
					flag = false
				}
			}
			//当所有MapTask执行完后生成ReduceTask
			if len(c.ReduceTask) == 0 && flag {
				c.m.Lock()
				//收集中间文件
				paths := make([]string, 0)
				for i := 0; i < len(c.MapTask); i++ {
					paths = append(paths, c.MapTask[i].IntermediateFilePaths...)
				}
				c.ReduceTask = []*MapReduceTask{
					&MapReduceTask{
						ID:                    c.IDGeranator,
						Type:                  TasktypeReduce,
						Status:                TaskStatusUnallocated,
						NReduceWorker:         c.NReduceWorker,
						IntermediateFilePaths: paths,
						OutputFilePath:        c.MROutFile,
					},
				}
				c.IDGeranator++
				c.m.Unlock()
			}
			for i := 0; i < len(c.ReduceTask); i++ {
				if c.ReduceTask[i].Status != TaskStatusFinished {
					flag = false
				}
			}
			if flag {
				c.State <- flag
				break
			}
			time.Sleep(time.Second * 10)
		}
	}()
	log.Printf("master starting...\n")
	Print(&c)
	c.server()
	return &c
}

func Print(c *Coordinator) {
	mapTasks, err := json.Marshal(c.MapTask)
	if err != nil {
		log.Printf("marshal failed, err:%v\n", err)
	}
	log.Printf("coordinator meta mapTasks: %v\n", string(mapTasks))

	reduceTasks, err := json.Marshal(c.ReduceTask)
	if err != nil {
		log.Printf("marshal failed, err:%v\n", err)
	}
	log.Printf("coordinator meta reduceTasks: %v\n", string(reduceTasks))

	log.Printf("coordinator meta id: %v, nReduce: %v \n", c.IDGeranator, c.NReduceWorker)

}
