package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		request := AcTaskRequest{}
		response := AcTaskResponse{}
		// func in coordinator, return value is bool , info in response struct
		ok := call("Coordinator.AssignTask", request, response)
		if ok {
			//	get a task choose diff func via task type
			if response.TaskT == MapTask {
				doMapTask(mapf, response)
			} else if response.TaskT == ReduceTask {
				doReduceTask(reducef, response)
			} else if response.TaskT == WaitTask {
				time.Sleep(5 * time.Second)
			} else {
				log.Fatalf("get unknown task type : %v", response.TaskT)
			}
		} else {
			//can't get a task, quit
			os.Exit(1)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(mapf func(string, string) []KeyValue, response AcTaskResponse) {
	//mapf -- Map(filename string, contents string)
	// in fact, filename is unnecessary, contents are all we needed
	log.Printf("Worker: get Map Task : %v", response.TaskId)
	fileName := response.FileName
	// acquire contents == open & read
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file : %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file : %v", fileName)
	}
	file.Close()
	//	transfer content to mapf
	kva := mapf(fileName, string(content))
	// key value => each reducer
	kvaBuf := make([][]KeyValue, response.ReducerNum)
	for i := 0; i < int(response.ReducerNum); i++ {
		kvaBuf[i] = []KeyValue{}
	}
	// calculate reducer should reduce part
	for _, kv := range kva {
		//use ihash(key) % NReduce to pick the reduce task for a given key.
		idx := ihash(kv.Key) % int(response.ReducerNum)
		kvaBuf[idx] = append(kvaBuf[idx], kv)
	}
	// range kvaBuf
	for i := 0; i < int(response.ReducerNum); i++ {
		//  naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
		intermediateFileName := fmt.Sprintf("mr-%d-%d", response.TaskId, i)
		// To ensure that nobody observes partially written files in the presence of crashes
		// the trick of using a temporary file and atomically renaming it once it is completely written
		tmpFile, err := os.CreateTemp("", intermediateFileName)
		if err != nil {
			log.Fatalf("create temp file err : %v", err)
		}
		//	 use Go's encoding/json package. To write key/value pairs to a **JSON** file
		//  enc := json.NewEncoder(file)
		//  for _, kv := ... {
		//    err := enc.Encode(&kv)
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kvaBuf[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encoder err : %v", err)
			}
		}
		tmpFile.Close()
		//	rename file
		os.Rename(tmpFile.Name(), intermediateFileName)
	}

	//	call TaskDone async, let coordinator update tasks
	log.Printf("Worker: map task %v done, call DoneTask\n", response.TaskId)
	go func(taskId uint) {
		request := DoneTaskRequest{}
		response := DoneTaskResponse{}
		ok := call("Coordinator.TaskDone", &request, &response)
		if !ok {
			log.Fatalf("call map done err")
		}
	}(response.TaskId)
}

func doReduceTask(reducef func(string, []string) string, response AcTaskResponse) {
	log.Printf("Worker: get reduce task %v ", response.TaskId)
	kva := make(map[string][]string)
	for i := 0; i < int(response.TaskNum); i++ {
		intermediateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(int(response.TaskId))
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
		file.Close()
	}
	var reduceOut []KeyValue
	for k, v := range kva {
		reduceOut = append(reduceOut, KeyValue{
			Key: k,
			// Reduce(key string, values []string) string
			Value: reducef(k, v),
		})
	}
	outPutFile := "mr-out-" + strconv.Itoa(int(response.TaskId))
	oFile, err := ioutil.TempFile("", outPutFile)
	if err != nil {
		log.Fatal(err)
	}

	for _, data := range reduceOut {
		fmt.Fprintf(oFile, "%v %v\n", data.Key, data.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
	oFile.Close()
	os.Rename(oFile.Name(), outPutFile)

	fmt.Printf("Worker: reduce task %v done, call DoneTask\n", response.TaskId)
	go func(taskId uint) {
		args := DoneTaskRequest{}
		reply := DoneTaskResponse{}
		args.TaskId = taskId
		args.TaskT = ReduceTask
		ok := call("Coordinator.TaskDone", &args, &reply)
		if !ok {
			log.Fatal("reduce call task done error!")
		}
	}(response.TaskId)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
