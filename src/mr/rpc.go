package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type GetTask struct {
	WorkerId int
}

type ReplyTask struct {
	File    []string `default:nil`
	NReduce int      `default:1`
	TaskId  int      `default:-1`
	Type    string   `default:""`
	Done    bool     `default:false`
}

type SendTaskReport struct {
	Status   bool
	Files    []string
	WorkerId int
	TaskId   int
	Type     string
}

type ReplyTaskReport struct {
	Acknowledgement bool `default: false`
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
