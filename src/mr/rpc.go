package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType uint8

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
)

//Since the task type is specified, diff rpc method is not required
// acquire task

type AcTaskRequest struct {
}
type AcTaskResponse struct {
	// task info
	TaskT  TaskType
	TaskId uint
	//	file name
	FileName   string
	ReducerNum uint
	TaskNum    uint
}

//Done task

type DoneTaskRequest struct {
	// task info
	TaskT  TaskType
	TaskId uint
}

type DoneTaskResponse struct {
}

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
