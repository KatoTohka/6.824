package mr

import (
	"errors"
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
	//lock for the struct
	mu                    sync.Mutex
	mapTaskUnallocated    map[int]bool
	mapTaskUndone         map[int]bool
	reduceTaskUnallocated map[int]bool
	reduceTaskUndone      map[int]bool
	files                 []string
	// num of reducers
	numReducers int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	// lock when operate on the maps of task info
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.mapTaskUnallocated) == 0 && len(c.mapTaskUndone) == 0 && len(c.reduceTaskUnallocated) == 0 && len(c.reduceTaskUndone) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.mapTaskUnallocated = make(map[int]bool)
	c.mapTaskUndone = make(map[int]bool)
	c.reduceTaskUnallocated = make(map[int]bool)
	c.reduceTaskUndone = make(map[int]bool)
	// file -> mapTask
	for i := 0; i < len(files); i++ {
		c.mapTaskUnallocated[i] = true
	}

	c.server()
	return &c
}

func (c *Coordinator) AcquireTask(args *AcTaskRequest, reply *AcTaskReply) error {
	//	assign a task to reply
	c.mu.Lock()
	defer c.mu.Lock()
	if len(c.mapTaskUnallocated) > 0 {
		for i := range c.mapTaskUnallocated {
			reply.TaskId = i
			reply.TaskT = MapTask
			reply.MapFileName = c.files[i]
			reply.NumReducer = c.numReducers
			delete(c.mapTaskUnallocated, i)
			c.mapTaskUndone[i] = true
			log.Printf("Coordinator: Assigned map task %v \n", i)
			// check whether finished in timeout
			go func(taskId int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.mapTaskUndone[taskId]
				if ok {
					//	timeout -> put back task to unallocated queue
					log.Printf("Coordinator: Map task %v time out", taskId)
					delete(c.mapTaskUndone, taskId)
					c.mapTaskUnallocated[taskId] = true
				}
			}(i)
			// done one and return
			return nil
		}

	} else if len(c.mapTaskUndone) > 0 {
		//	wait
		reply.TaskT = WaitTask
		return nil
	} else if len(c.reduceTaskUnallocated) > 0 {
		for i := range c.reduceTaskUnallocated {
			reply.TaskId = i
			reply.TaskT = ReduceTask
			reply.MapTaskNum = len(c.files)

			delete(c.reduceTaskUnallocated, i)
			c.reduceTaskUndone[i] = true
			log.Printf("Coordinator:Assigned reduce task %v\n", i)
			// check whether finished in timeout
			go func(taskId int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.reduceTaskUndone[taskId]
				if ok {
					//	timeout -> put back task to unallocated queue
					log.Printf("Coordinator: Reduce task %v time out", taskId)
					delete(c.reduceTaskUndone, taskId)
					c.reduceTaskUnallocated[taskId] = true
				}
			}(i)
			return nil
		}
	} else if len(c.reduceTaskUndone) > 0 {
		//	should wait
		reply.TaskT = WaitTask
		return nil
	}
	// if all maps are empty, return err to stop the worker
	return errors.New("coordinator: No more task to be assigned.All job done")

}

func (c *Coordinator) TaskDone(args *DoneTaskRequest, reply *DoneTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskT == MapTask {
		_, ok := c.mapTaskUndone[args.TaskId]
		if ok {
			//	the task is in UnDone queue
			delete(c.mapTaskUndone, args.TaskId)
		} else {
			_, ok := c.mapTaskUnallocated[args.TaskId]
			if ok {
				// the task is in Unallocated queue, happened when there is timeout and then send reply
				delete(c.mapTaskUnallocated, args.TaskId)
			}
		}
		log.Printf("Coordinator: map task %v done \n", args.TaskId)
		//	check whether all map task is done and then begin reduce task
		if len(c.mapTaskUnallocated) == 0 && len(c.mapTaskUndone) == 0 {
			for i := 0; i < c.numReducers; i++ {
				c.reduceTaskUnallocated[i] = true
			}
		}
	} else { // reduce task
		_, ok := c.reduceTaskUndone[args.TaskId]
		if ok {
			delete(c.reduceTaskUndone, args.TaskId)
		} else {
			_, ok := c.reduceTaskUnallocated[args.TaskId]
			if ok {
				delete(c.reduceTaskUnallocated, args.TaskId)
			}
		}
		log.Printf("Coordinator: map task %v done \n", args.TaskId)

	}
	return nil
}
