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

// Add your RPC definitions here.

type Args struct {
	WorkerMappingRequested bool
	WorkerMappingFinished  bool

	WorkerReducingRequested bool
	WorkerReducingFinished  bool

	MappingTaskNumber  int
	ReducingTaskNumber int

	FileName string
}

type Reply struct {
	NumberOfFiles int

	WorkerMappingFinished   bool
	WorkerReducingFinished  bool
	OverallMappingFinished  bool
	OverallReducingFinished bool

	TaskAssigned bool

	FileName        string
	NumOfPartitions int

	MappingTaskNumber  int
	ReducingTaskNumber int

	ReducingIntermediateFileList []string

	PartitionFileList []string
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
