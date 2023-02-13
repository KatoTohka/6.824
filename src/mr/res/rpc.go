package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 将任务分为三类

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
)

// 获取任务 acquire task

type AcTaskRequest struct {
}

type AcTaskReply struct {
	TaskT  TaskType // task type
	TaskId int      // task number
	//	input file names
	NumReducer  int
	MapFileName string
	MapTaskNum  int
}

// 完成任务 done task

type DoneTaskRequest struct {
	TaskT  TaskType // task type
	TaskId int      // task number
}

type DoneTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
