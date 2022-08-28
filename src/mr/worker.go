package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func (kv *KeyValue) String() string {
	return kv.Key + "-" + kv.Value
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for true {
		req := &GetTaskArgs{Addr: "localhost"}
		resp := &GetTaskReply{}
		ok := call("Coordinator.GetTask", req, resp)
		log.Printf("req: %v, resp: %v", Any2String(req), Any2String(resp))
		if ok && resp.Task != nil {

			if resp.Code == CodeShutdown {
				log.Printf("shut down...")
				return
			}
			processTask(resp.Task, mapf, reducef)
		} else if !ok {
			fmt.Printf("call failed\n")
		}
		time.Sleep(time.Second * 10)
	}
}

func processTask(t *MapReduceTask, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {

	log.Printf("process task starting...\n")
	switch t.Type {
	case TasktypeMap:
		err := processTaskForMap(t, mapf)
		if err != nil {
			log.Fatalf("failed to map, task: %v\n", t)
			return err
		}
		UpdateTask(t)
	case TasktypeReduce:
		err := processTaskForReduce(t, reducef)
		if err != nil {
			log.Fatalf("failed to reduce, task: %v\n", t)
			return err
		}
		UpdateTask(t)
	}
	return nil
}

func processTaskForMap(t *MapReduceTask, mapf func(string, string) []KeyValue) error {
	filename := t.SourceFilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kvs := mapf(filename, string(content))

	reduceAddrs := t.IntermediateFilePaths
	//open中间文件
	files := make([]*os.File, 0, len(reduceAddrs))
	encoders := make([]*json.Encoder, 0, t.NReduceWorker)
	for i := 0; i < t.NReduceWorker; i++ {
		mfile, err := os.OpenFile(t.IntermediateFilePaths[i], os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			break
		}
		files = append(files, mfile)
		encoder := json.NewEncoder(mfile)
		encoders = append(encoders, encoder)
	}
	defer func() {
		for i := 0; i < len(reduceAddrs); i++ {
			files[i].Sync()
			files[i].Close()
		}
	}()
	//写入到中间文件
	for _, kv := range kvs {
		hash := ihash(kv.Key) % t.NReduceWorker
		encoders[hash].Encode(kv)
	}
	log.Printf("kv: %v", len(kvs))
	return nil
}

//统计各word数量，相同的word必然写入到同一个中间文件
func processTaskForReduce(t *MapReduceTask, reducef func(string, []string) string) error {

	intermediateFilePaths := t.IntermediateFilePaths
	// key: word , value: kvs
	kvMaps := map[string][]string{}
	//open中间文件
	files := make([]*os.File, 0, len(intermediateFilePaths))
	for i := 0; i < len(intermediateFilePaths); i++ {
		mfile, err := os.OpenFile(intermediateFilePaths[i], os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilePaths[i])
		} else {
			files = append(files, mfile)
		}
		//从每个中间文件读取k-v，并统计其value
		doc := json.NewDecoder(files[i])
		for true {
			var kv KeyValue
			if err = doc.Decode(&kv); err != nil {
				fmt.Printf("decode failed in reducing, err: %v", err)
				break
			}
			if _, ok := kvMaps[kv.Key]; !ok {
				kvMaps[kv.Key] = make([]string, 10)
			}
			kvMaps[kv.Key] = append(kvMaps[kv.Key], kv.Value)
		}
	}
	defer func() {
		for i := 0; i < len(files); i++ {
			files[i].Close()
		}
	}()

	res := make([]string, 0, len(kvMaps))
	for key, values := range kvMaps {
		res = append(res, fmt.Sprintf("%v %v\n", key, reducef(key, values)))
	}
	sort.Strings(res)
	log.Printf("output: %v", res)
	oname := "mr-out-0"
	if err := ioutil.WriteFile(oname, []byte(strings.Join(res, "")), 0777); err != nil {
		log.Fatalf("write output failed, file: %v", oname)
		return err
	}
	return nil
}

func UpdateTask(task *MapReduceTask) error {

	req := &UpdateTaskArgs{}
	req.Task = task
	resp := &UpdateTaskReply{}
	ok := call("Coordinator.UpdateTask", req, resp)
	if !ok {
		fmt.Printf("call failed!\n")
		return errors.New("call failed, method: UpdateTask")
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
