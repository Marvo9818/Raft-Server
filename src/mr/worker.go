package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		args := Args{}
		args.FileName = ""
		args.WorkerMappingRequested = true
		args.WorkerReducingRequested = false

		reply := Reply{}

		// Send a request to the coordinator for mapping task to be assigned
		RequestMapping(&args, &reply)

		// Get the response from the coordinator to determine if all of the mapping tasks are finished.
		mappingFinished := reply.OverallMappingFinished

		// Moving on to reducing phase if all of the mapping tasks are finished
		if mappingFinished {
			break
		} else {
			taskAssigned := reply.TaskAssigned

			// If the mapping task is assigned, then start the mapping phase, otherwise keeps sending request to coordinator.
			if taskAssigned {
				fileName := reply.FileName
				numOfPartitions := reply.NumOfPartitions

				mapNumber := reply.MappingTaskNumber

				// Mapping function to map a input file into several partition files.
				Map(mapf, fileName, numOfPartitions, mapNumber)

				args := Args{}
				args.WorkerMappingFinished = true
				args.WorkerMappingRequested = false
				args.WorkerReducingRequested = false
				args.MappingTaskNumber = mapNumber

				reply := Reply{}

				// Send a request to coordinator once the mapping is finished/
				MappingFinished(&args, &reply)

				// Wait for one second.
				time.Sleep(1000)
			}
		}
	}

	for {
		args := Args{}
		args.FileName = ""
		args.WorkerMappingRequested = false
		args.WorkerReducingRequested = true

		reply := Reply{}

		// Send a request to the coordinator for reducing task to be assigned
		RequestReducing(&args, &reply)
		reducingFinished := reply.OverallReducingFinished

		// End the program if all of the reducing tasks are finished
		if reducingFinished {
			break
		} else {
			taskAssigned := reply.TaskAssigned

			// If the reducing task is assigned, then start the reducing, otherwise keeps sending request to coordinator.
			if taskAssigned {
				reduceNumber := reply.ReducingTaskNumber
				numOfInputFiles := reply.NumberOfFiles

				fileName := "mr-out-" + strconv.FormatInt(int64(reduceNumber), 10)

				// Reducing function to reduce several partition files into "mr-out-X" where X stnads for reduce task number.
				Reduce(reducef, reduceNumber, numOfInputFiles, fileName)

				args := Args{}
				args.WorkerMappingFinished = true
				args.WorkerReducingFinished = true
				args.WorkerMappingRequested = false
				args.WorkerReducingRequested = false
				args.ReducingTaskNumber = reduceNumber

				reply := Reply{}

				// Send a response to coordinator once the mapping is finished.
				MappingFinished(&args, &reply)

				// Wait for one second.
				time.Sleep(1000)
			}
		}
	}

}

/* Function of sending request to coordinator */
func RequestMapping(args *Args, reply *Reply) {
	ok := call("Coordinator.RPCHandler", &args, &reply)

	if ok {

	} else {
		fmt.Println("Request Not Successful!")
	}
}

/* Function of sending reponse to coordinator once mapping is finished. */
func MappingFinished(args *Args, reply *Reply) {
	ok := call("Coordinator.RPCHandler", &args, &reply)

	if ok {
	} else {
		fmt.Println("Request Not Successful!")
	}
}

/*
Mapping function
*/
func Map(mapf func(string, string) []KeyValue, filename string, numOfPartitions int, mapNumber int) {

	intermediate := []KeyValue{}

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
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	ofile_arr := [][]KeyValue{}

	for i := 0; i < numOfPartitions; i++ {
		ofile_arr = append(ofile_arr, []KeyValue{})
	}

	/// Array ofile where index refers to the partition number
	for i := 0; i < len(intermediate); i++ {
		word := intermediate[i].Key
		partitionNum := ihash(word) % numOfPartitions

		ofile_arr[partitionNum] = append(ofile_arr[partitionNum], intermediate[i])
	}

	for i := 0; i < numOfPartitions; i++ {
		tempfilename := "temp-" + strconv.FormatInt(int64(mapNumber), 10) + "-" + strconv.FormatInt(int64(i), 10)
		oname := "mr-" + strconv.FormatInt(int64(mapNumber), 10) + "-" + strconv.FormatInt(int64(i), 10)

		ofile, _ := ioutil.TempFile(".", tempfilename)

		enc := json.NewEncoder(ofile)

		for j := 0; j < len(ofile_arr[i]); j++ {
			err = enc.Encode(&ofile_arr[i][j])
		}

		os.Rename(ofile.Name(), oname)

		ofile.Close()
	}

}

/*
Send reqeust to coordinator for getting a task of reducing mapped files.
*/
func RequestReducing(args *Args, reply *Reply) {

	ok := call("Coordinator.RPCHandler", &args, &reply)

	if ok {
	} else {
		fmt.Println("Request Not Successful!")
	}
}

/*
Send request to coordinator when a reduce task is completed.
*/
func ReduceFinished(args *Args, reply *Reply) {
	ok := call("Coordinator.RPCHandler", &args, &reply)

	if ok {
	} else {
		fmt.Println("Request Not Successful!")
	}
}

/*
Reducing the partitioned files based on the reduce task number.
*/
func Reduce(reducef func(string, []string) string, mapReduceNumber int, numOfInputFiles int, fileName string) {

	var kva []KeyValue

	for i := 0; i < numOfInputFiles; i++ {
		filename := "mr-" + strconv.FormatInt(int64(i), 10) + "-" + strconv.FormatInt(int64(mapReduceNumber), 10)
		ofile, _ := os.Open(filename)

		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	tempfilename := "temp-" + strconv.FormatInt(int64(mapReduceNumber), 10)
	oname := "mr-out-" + strconv.FormatInt(int64(mapReduceNumber), 10)

	ofile, _ := ioutil.TempFile(".", tempfilename)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)

	ofile.Close()

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
