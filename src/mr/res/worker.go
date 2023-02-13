package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
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

	// worker should loop until call return false : no task any more
	for {
		args := AcTaskRequest{}
		reply := AcTaskReply{}
		// try to get a task from coordinator
		// request struct -> args | reply struct -> reply
		ok := call("Coordinator.AcquireTask", &args, &reply)
		if ok {
			//	get a task
			//	carry out task based on task types
			//	two kind task correspond to two func
			if reply.TaskT == MapTask {
				doMapTask(reply, mapf)
			} else if reply.TaskT == ReduceTask {
				doReduceTask(reply, reducef)
			} else if reply.TaskT == WaitTask {
				time.Sleep(1 * time.Second)
			} else {
				// log.Fatalf contains os.Exit
				log.Fatalf("unknown task type \n")
			}
		} else {
			//	can't get a task , quit
			os.Exit(1)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// do Map Task
func doMapTask(reply AcTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("Worker: get map task %v \n", reply.TaskId)
	fileName := reply.MapFileName
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	// content is the text needed to map  type : []byte
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	// mapf(string, string) return result => kva
	kva := mapf(fileName, string(content))
	// ？
	intermediate := make([][]KeyValue, reply.NumReducer)
	for i := 0; i < reply.NumReducer; i++ {
		intermediate[i] = []KeyValue{}
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.NumReducer
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for i := 0; i < reply.NumReducer; i++ {
		outFileName := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
		// return *File
		tmpFile, err := os.CreateTemp("", outFileName)
		// for loop使用defer可能存在资源泄露.
		//defer tmpFile.Close()
		if err != nil {
			log.Fatalf("create temp file err %v", err)
		}
		newEncoder := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			err := newEncoder.Encode(&kv)
			if err != nil {
				log.Fatalf("encode err %v", err)
			}
		}
		tmpFile.Close()
		// rename操作是原子的
		os.Rename(tmpFile.Name(), outFileName)

	}
	//	call TaskDone , let coordinator update the task
	log.Printf("Worker: map taks %v done, call DoneTask\n", reply.TaskId)
	go func(taskId int) {
		DoneArgs := DoneTaskRequest{}
		DoneReply := DoneTaskReply{}
		DoneArgs.TaskT = MapTask
		ok := call("Coordinator.TaskDone", &DoneArgs, &DoneReply)
		if !ok {
			log.Fatalf("call map done error")
		}
	}(reply.TaskId)
}

//do Reduce task
func doReduceTask(reply AcTaskReply, reducef func(string, []string) string) {
	log.Printf("Worker: get reduce task %v\n", reply.TaskId)
	intermediate := make(map[string][]string)
	for i := 0; i < reply.MapTaskNum; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		newDecoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := newDecoder.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
		file.Close()
	}
	var reduceOut []KeyValue
	for k, v := range intermediate {
		reduceOut = append(reduceOut, KeyValue{k, reducef(k, v)})
	}
	outPutFile := "mr-out-" + strconv.Itoa(reply.TaskId)
	tmpFile, err := os.CreateTemp("", outPutFile)
	if err != nil {
		log.Fatalf("create temp file err, %v", err)
	}
	for _, data := range reduceOut {
		fmt.Fprintf(tmpFile, "%v %v\n", data.Key, data.Value)
		if err != nil {
			log.Fatalf("write file err, %v", err)
		}
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), outPutFile)
	log.Printf("Worker: reduce file %v  done, call DoneTask", reply.TaskId)
	go func(taskId int) {
		args := DoneTaskRequest{}
		reply := DoneTaskReply{}
		args.TaskId = taskId
		args.TaskT = ReduceTask
		ok := call("Coordinator.TaskDone", &args, &reply)
		if !ok {
			log.Fatalf("reduce call taskDone error")
		}
	}(reply.TaskId)

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
