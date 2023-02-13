package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

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

	//worker should get itself a task in a loop until all tasks are finished
	for {
		args := AcTaskArgs{}
		reply := AcTaskReply{}

		ok := call("Coordinator.AcquireTask", &args, &reply) // try to get a task from the coordinator

		if ok {
			//get a task
			//carry out task based on task types
			if reply.Task_t == MapTask {

				doMapTask(reply, mapf)

			} else if reply.Task_t == ReduceTask {
				doReduceTask(reply, reducef)

			} else if reply.Task_t == WaitTask {
				time.Sleep(5 * time.Second)
			} else {
				log.Fatalf("unknown task type\n")
				os.Exit(1)
			}

		} else {
			//can't get task, quit
			os.Exit(1)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduceTask(reply AcTaskReply, reducef func(string, []string) string) {

	fmt.Printf("Worker: get reduce task %v\n", reply.Task_id)
	allData := make(map[string][]string)
	for i := 0; i < reply.Map_task_num; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Task_id)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			allData[kv.Key] = append(allData[kv.Key], kv.Value)
		}
		file.Close()
	}

	var reduceOut []KeyValue
	for k, v := range allData {
		tmpData := KeyValue{k, reducef(k, v)}
		reduceOut = append(reduceOut, tmpData)
	}

	outPutFile := "mr-out-" + strconv.Itoa(reply.Task_id)
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

	fmt.Printf("Worker: reduce task %v done, call DoneTask\n", reply.Task_id)
	go func(task_id int) {
		args := DoneTaskArgs{}
		reply := DoneTaskReply{}
		args.TaskId = task_id
		args.TaskT = ReduceTask
		ok := call("Coordinator.TaskDone", &args, &reply)
		if !ok {
			log.Fatal("reduce call taskdone error!")
		}
	}(reply.Task_id)
}

func doMapTask(reply AcTaskReply, mapf func(string, string) []KeyValue) {

	fmt.Printf("Worker: get map task %v \n", reply.Task_id)
	filename := reply.Map_file_name
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	tmpCont := make([][]KeyValue, reply.Num_reducer)

	for i := 0; i < reply.Num_reducer; i++ {
		tmpCont[i] = []KeyValue{}
	}
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.Num_reducer
		tmpCont[idx] = append(tmpCont[idx], kv)
	}

	for i := 0; i < reply.Num_reducer; i++ {
		outFileName := fmt.Sprintf("mr-%d-%d", reply.Task_id, i)
		oFile, err := ioutil.TempFile("", outFileName)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(oFile)
		for _, kv := range tmpCont[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		oFile.Close()
		// rename file atomic
		os.Rename(oFile.Name(), outFileName)
	}

	//call TaskDone async, let coordinator update the tasks
	fmt.Printf("Worker: map task %v done, call DoneTask\n", reply.Task_id)
	go func(taskId int) {
		DoneArgs := DoneTaskArgs{}
		DoneReply := DoneTaskReply{}
		DoneArgs.TaskT = MapTask
		DoneArgs.TaskId = taskId
		ok := call("Coordinator.TaskDone", &DoneArgs, &DoneReply)
		if !ok {
			log.Fatal("call map done error!")
		}
	}(reply.Task_id)

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
