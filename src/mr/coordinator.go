package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	InProgress           bool
	NumOfTasksInProgress int

	NumOfMappingTasksAssigned   int
	NumOfMappingTasksFinished   int
	NumOfMappingTasksInProgress int
	MappingFinished             bool

	NumOfReducingTasksAssigned   int
	NumOfReducingTasksFinished   int
	NumOfReducingTasksInProgress int
	ReducingFinished             bool

	NumOfTasksAssigned int
	NumOfTasksFinished int

	MappingReceived  bool
	ReducingReceived bool

	// One of Them
	CoordinatorLock sync.Mutex

	// Indicies -> Values used to track if they are completed or not

	Files []string

	FinishedMappingTasks  []bool
	FinishedReducingTasks []bool

	/// Status figure out different stages, doesn't matter if it is bool, variable as long as there is
	// a way to classify then

	//Mapping File
	/// Three status: "available", "in progress", "completed"
	MappingTasksStatus  []string
	ReducingTasksStatus []string
	WorkerInProgress    []string

	NumOfPartitions int

	// Mechanics to track different processes.
	MappingStartingTimeArr  []time.Time
	ReducingStartingTimeArr []time.Time
	WorkerStartingTimeArr   []time.Time

	//
	CurrentStage string
	ShuffuleNum  int
}

// Your code here -- RPC handlers for the worker to call.

/*
What should handler do:

1. See if the progress is going
2. Send back the progress
3. Send different message
4. Substitute as go routine
5.
*/
func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {
	// Partition the files to the worker

	c.CoordinatorLock.Lock()
	// If a worker sends out the request while the file remains to be sent to worker, the send this file to the worker.
	if args.WorkerMappingRequested && c.NumOfMappingTasksAssigned < len(c.Files) {

		assignedTask := false

		for i := 0; i < len(c.Files); i++ {
			if c.MappingTasksStatus[i] == "available" {
				assignedTask = true
				reply.FileName = c.Files[i]
				reply.MappingTaskNumber = i
				reply.NumOfPartitions = c.NumOfPartitions
				reply.TaskAssigned = true
				c.NumOfMappingTasksAssigned++
				c.MappingTasksStatus[i] = "in progress"
				c.MappingStartingTimeArr[i] = time.Now()
				break
			}
		}

		if !assignedTask {
			reply.TaskAssigned = false
		}
		/// If none of the files have tasks assigned, then it will be likely that:
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	// If a worker completes a mapping, then increments the amount of mapping finished by 1.
	if args.WorkerMappingFinished && !c.MappingFinished {
		c.NumOfMappingTasksFinished++
		c.FinishedMappingTasks[args.MappingTaskNumber] = true
		c.MappingTasksStatus[args.MappingTaskNumber] = "finished"
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	// If the all the mapping tasks of all the files are compeleted, sends true so that it will move onto next stage of
	if c.NumOfMappingTasksFinished >= len(c.Files) {
		c.MappingFinished = true
		reply.OverallMappingFinished = true
		c.CurrentStage = "Reducing"
		// c.FinishedMappingFiles = append(c.FinishedMappingFiles, reply.FileName)
	}
	c.CoordinatorLock.Unlock()

	//// Reducing Part ranging from 0 to nPartition/nReduce - 1

	c.CoordinatorLock.Lock()
	if args.WorkerReducingRequested && c.NumOfReducingTasksAssigned < c.NumOfPartitions {
		assignedTask := false

		for i := 0; i < c.NumOfPartitions; i++ {
			if c.ReducingTasksStatus[i] == "available" {
				assignedTask = true
				reply.ReducingTaskNumber = i
				reply.NumberOfFiles = len(c.Files)
				reply.TaskAssigned = true
				c.NumOfReducingTasksAssigned++
				c.ReducingTasksStatus[i] = "in progress"
				c.ReducingStartingTimeArr[i] = time.Now()
				break
			}
		}

		if !assignedTask {
			reply.TaskAssigned = false
		}
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	if args.WorkerReducingFinished && !c.ReducingFinished {
		c.NumOfReducingTasksFinished++
		c.FinishedReducingTasks[args.ReducingTaskNumber] = true
		c.ReducingTasksStatus[args.ReducingTaskNumber] = "finished"
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	if c.NumOfReducingTasksFinished >= c.NumOfPartitions {
		c.ReducingFinished = true
		reply.OverallReducingFinished = true
	}
	c.CoordinatorLock.Unlock()

	return nil
}

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

	c.CoordinatorLock.Lock()

	if c.CurrentStage == "Mapping" && c.NumOfMappingTasksAssigned > 0 {
		for i := 0; i < len(c.MappingTasksStatus); i++ {
			if time.Since(c.MappingStartingTimeArr[i]).Seconds() > 10 && c.MappingTasksStatus[i] == "in progress" {
				c.NumOfMappingTasksAssigned--
				c.MappingTasksStatus[i] = "available"
			}
		}
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	if c.CurrentStage == "Reducing" && c.NumOfReducingTasksAssigned > 0 {
		for i := 0; i < len(c.ReducingTasksStatus); i++ {
			if time.Since(c.ReducingStartingTimeArr[i]).Seconds() > 10 && c.ReducingTasksStatus[i] == "in progress" {
				c.NumOfReducingTasksAssigned--
				c.ReducingTasksStatus[i] = "available"
			}
		}
	}
	c.CoordinatorLock.Unlock()

	c.CoordinatorLock.Lock()
	if c.ReducingFinished {
		ret = true
	}
	c.CoordinatorLock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.Files = files
	c.NumOfPartitions = nReduce

	c.NumOfTasksAssigned = 0
	c.NumOfTasksFinished = 0

	c.NumOfMappingTasksAssigned = 0
	c.NumOfMappingTasksInProgress = 0
	c.NumOfMappingTasksFinished = 0
	c.MappingFinished = false

	c.NumOfReducingTasksAssigned = 0
	c.NumOfReducingTasksInProgress = 0
	c.NumOfReducingTasksFinished = 0
	c.ReducingFinished = false
	c.CurrentStage = "Mapping"

	for i := 0; i < len(files); i++ {
		c.MappingStartingTimeArr = append(c.MappingStartingTimeArr, time.Time{})
		c.FinishedMappingTasks = append(c.FinishedMappingTasks, false)
		c.MappingTasksStatus = append(c.MappingTasksStatus, "available")
	}

	for i := 0; i < nReduce; i++ {
		c.ReducingStartingTimeArr = append(c.ReducingStartingTimeArr, time.Time{})
		c.FinishedReducingTasks = append(c.FinishedReducingTasks, false)
		c.ReducingTasksStatus = append(c.ReducingTasksStatus, "available")
	}

	c.server()
	return &c
}
