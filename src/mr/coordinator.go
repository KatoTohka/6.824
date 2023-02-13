package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Coordinator should be responsible for assigning tasks and files to mrapp
type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex //lock for the struct

	mapTaskUnallocated    map[int]bool
	mapTaskUndone         map[int]bool
	reduceTaskUnallocated map[int]bool
	reduceTaskUndone      map[int]bool

	files []string

	numReducer int //reducer的个数，在MakeCoordinator里设置,被 main/mrcoordinator call
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.

	//lock when operate on the maps of task info
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTaskUnallocated) == 0 && len(c.mapTaskUndone) == 0 && len(c.reduceTaskUnallocated) == 0 && len(c.reduceTaskUndone) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.mapTaskUnallocated = make(map[int]bool)
	c.mapTaskUndone = make(map[int]bool)
	c.reduceTaskUnallocated = make(map[int]bool)
	c.reduceTaskUndone = make(map[int]bool)
	c.numReducer = nReduce

	//file -> mapTask
	for i := 0; i < len(files); i++ {
		c.mapTaskUnallocated[i] = true
	}

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) AssignTask(args *AcTaskRequest, reply *AcTaskResponse) error {

	//assign a task to reply
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTaskUnallocated) > 0 {
		for i := range c.mapTaskUnallocated {
			reply.TaskId = uint(i)
			reply.TaskT = MapTask
			reply.FileName = c.files[i]
			reply.ReducerNum = uint(c.numReducer)

			delete(c.mapTaskUnallocated, i)
			c.mapTaskUndone[i] = true

			fmt.Printf("Coordinator: Assigned map task %v\n", i)

			//check whether finished in timeout
			go func(taskId int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.mapTaskUndone[taskId]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("Coordinator: Map Task", taskId, "time out!")
					delete(c.mapTaskUndone, taskId)
					c.mapTaskUnallocated[taskId] = true
				}
			}(i)

			return nil
		}
	} else if len(c.mapTaskUndone) > 0 {
		//should wait
		reply.TaskT = WaitTask
		return nil

	} else if len(c.reduceTaskUnallocated) > 0 {
		for i := range c.reduceTaskUnallocated {
			reply.TaskId = uint(i)
			reply.TaskT = ReduceTask
			reply.ReducerNum = uint(len(c.files))

			delete(c.reduceTaskUnallocated, i)
			c.reduceTaskUndone[i] = true
			fmt.Printf("Coordinator: Assigned reduce task %v\n", i)

			//check whether finished in timeout
			go func(taskId int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.reduceTaskUndone[taskId]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("Coordinator: Reduce Task", taskId, "time out!")
					delete(c.reduceTaskUndone, taskId)
					c.reduceTaskUnallocated[taskId] = true
				}
			}(i)

			return nil
		}

	} else if len(c.reduceTaskUndone) > 0 {
		//should wait
		reply.TaskT = WaitTask
		return nil
	}

	//all maps are empty, return an err to stop the worker
	return errors.New("coordinator: No more task to be assigned. All job done")
}

func (c *Coordinator) TaskDone(args *DoneTaskRequest, reply *DoneTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskT == MapTask {
		_, ok := c.mapTaskUndone[int(args.TaskId)]
		if ok {
			// the task is in the NoDone queue
			delete(c.mapTaskUndone, int(args.TaskId))

		} else {
			_, ok := c.mapTaskUnallocated[int(args.TaskId)]
			if ok {
				// the taskID is found in Unallocated, this can happen when this is timeout and then send reply
				delete(c.mapTaskUnallocated, int(args.TaskId))
			}
		}

		fmt.Printf("Coordinator: map task %v done\n", args.TaskId)

		// check whether all map task is done and then begin reduce task
		if len(c.mapTaskUnallocated) == 0 && len(c.mapTaskUndone) == 0 {
			for i := 0; i < c.numReducer; i++ {
				c.reduceTaskUnallocated[i] = true
			}
		}
	} else {
		_, ok := c.reduceTaskUndone[int(args.TaskId)]
		if ok {
			delete(c.reduceTaskUndone, int(args.TaskId))
		} else {
			_, ok := c.reduceTaskUnallocated[int(args.TaskId)]
			if ok {
				delete(c.reduceTaskUnallocated, int(args.TaskId))
			}
		}
		fmt.Printf("Coordinator: reduce task %v done\n", args.TaskId)
	}
	return nil

}
