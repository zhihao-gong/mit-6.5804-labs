package mr

//
// RPC definitions.
//

import (
	"os"
	"strconv"
)

type RegisterArgs struct {
}

type RegisterReply struct {
	Code      int
	AssgnedId string
	Message   string
}

type AskForTaskArgs struct {
	Status WorkerStatus
}

type AskForTaskReply struct {
	Code    int
	Message string
	Task    Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
