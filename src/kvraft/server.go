package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name        string // Name of the Operation
	Key         string // The Key Used in the method
	Value       string // The Value to be Put in Get Method
	ClientID    int    // The ID of client requesting for this operation.
	SerialNumID int    // The ID of client transcation
	StartIndex  int    // The index returned called by Start()
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValuePairs           map[string]string
	readCh                  chan interface{}
	committedMsg            bool
	executedSerialNum       int    // The serial number whose request is already executed
	latestReply             string // The last response for write
	executedRequestNum      map[int]int
	executedGetRequestReply map[int]string
	executedStartOrder      []int // The order that Start gets called
	clientreadCh            map[int]chan interface{}
	committedCommands       []Op
	commitIndex             int
	nextCommitIndex         int
	clientResultCh          map[int]chan string
	clientOps               map[int]Op
	clientStartIndex        map[int]int

	clientGetCh               map[int]chan GetReply
	clientPutAppendCh         map[int]chan PutAppendReply
	committedEntriesAvailable bool
	clientResponses           map[int]interface{}
	clientDuplicates          map[int]bool

	timeout          time.Duration
	totalRunTime     time.Duration
	totalRunRequests int

	requestStartTime map[int]time.Time

	clientRequestMap map[int]Op
	commandIndex     int
}

func (kv *KVServer) readChannel() {
	time.Sleep(10 * time.Second)
	kv.readCh = make(chan interface{})
	kv.readCh <- 1
	close(kv.readCh)
}

func (kv *KVServer) closeChannel(op Op) {
	time.Sleep(10 * time.Millisecond)

	kv.mu.Lock()
	if op.Name == "Get" {
		reply := GetReply{}
		reply.Err = ErrWrongLeader
		reply.Value = ""
		kv.clientGetCh[op.ClientID] <- reply
		close(kv.clientGetCh[op.ClientID])
	} else {
		reply := PutAppendReply{}
		reply.Err = ErrWrongLeader
		kv.clientPutAppendCh[op.ClientID] <- reply
		close(kv.clientPutAppendCh[op.ClientID])
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//// Stale Requests detected
	kv.mu.Lock()
	serialNumReply, reponses_exist := kv.clientResponses[args.SerialNum]
	executedRequest := false

	if reponses_exist {
		reply.Err = serialNumReply.(GetReply).Err
		reply.Value = serialNumReply.(GetReply).Value
		executedRequest = true
	}
	kv.mu.Unlock()

	if !executedRequest {
		op := Op{}
		op.Name = "Get"
		op.Key = args.Key
		op.Value = ""
		op.ClientID = args.ClientID
		op.SerialNumID = args.SerialNum

		finished := false

		index, initialTerm, isLeader := kv.rf.Start(op)
		time.Sleep(1 * time.Millisecond)

		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.Value = ""
		} else {

			startTime := time.Now()

			for !finished {
				time.Sleep(500 * time.Microsecond)

				kv.mu.Lock()
				if index < len(kv.committedCommands) {
					if kv.committedCommands[index] != op {
						reply.Err = ErrWrongLeader
						reply.Value = ""
						finished = true
					} else {
						clientResponse := kv.clientResponses[args.SerialNum]
						reply.Err = clientResponse.(GetReply).Err
						reply.Value = clientResponse.(GetReply).Value
						finished = true
					}
				}
				kv.mu.Unlock()

				if time.Since(startTime) > 150*time.Millisecond {
					currentTerm, isLeader := kv.rf.GetState()
					time.Sleep(10 * time.Microsecond)

					if !isLeader {
						reply.Err = ErrWrongLeader
						reply.Value = ""
						finished = true
					} else {
						if currentTerm > initialTerm {
							_, term, isLeader := kv.rf.Start(op)
							time.Sleep(1 * time.Millisecond)

							if !isLeader {
								reply.Err = ErrWrongLeader
								reply.Value = ""
								finished = true
							} else {
								initialTerm = term
								startTime = time.Now()
							}
						} else {
							startTime = time.Now()
						}
					}
				}
			}
		}

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	executedRequest := false
	serialNumReply, reponses_exist := kv.clientResponses[args.SerialNum]
	if reponses_exist {
		reply.Err = serialNumReply.(PutAppendReply).Err
		executedRequest = true
	}
	kv.mu.Unlock()

	if !executedRequest {
		op := Op{}
		op.Name = args.Op
		op.Key = args.Key
		op.Value = args.Value
		op.ClientID = args.ClientID
		op.SerialNumID = args.SerialNum

		finished := false
		// resendTimes := 0
		// timeOut := 0

		index, initialTerm, isLeader := kv.rf.Start(op)
		time.Sleep(1 * time.Millisecond)

		if !isLeader {
			reply.Err = ErrWrongLeader
		} else {
			startTime := time.Now()

			for !finished {
				// time.Sleep(10 * time.Microsecond)
				time.Sleep(500 * time.Microsecond)

				kv.mu.Lock()
				// sendNewCommand := false
				if index < len(kv.committedCommands) {
					if kv.committedCommands[index] != op {
						reply.Err = ErrWrongLeader
						finished = true
					} else {
						clientResponse := kv.clientResponses[args.SerialNum]
						reply.Err = clientResponse.(PutAppendReply).Err
						finished = true
					}
					// fmt.Println("The time to finish is: ", time.Since(totalStartTime))
				}

				kv.mu.Unlock()

				if time.Since(startTime) > 150*time.Millisecond {
					currentTerm, isLeader := kv.rf.GetState()
					time.Sleep(10 * time.Microsecond)
					// time.Sleep(500 * time.Microsecond)
					if !isLeader {
						reply.Err = ErrWrongLeader
						finished = true
					} else {
						if currentTerm > initialTerm {
							_, term, isLeader := kv.rf.Start(op)
							time.Sleep(1 * time.Millisecond)

							// time.Sleep(500 * time.Microsecond)

							if !isLeader {
								reply.Err = ErrWrongLeader
								finished = true
							} else {
								initialTerm = term
								startTime = time.Now()
							}
						} else {
							startTime = time.Now()
						}
					}
				}
			}
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyStateMachine() {

	// committed := false
	for kv.killed() == false {
		// time.Sleep(10 * time.Millisecond)
		committedEntriesArr := make([]raft.ApplyMsg, 0)
		finished := false

		for !finished {
			time.Sleep(10 * time.Microsecond)
			select {
			case committedEntry := <-kv.applyCh:
				committedEntriesArr = append(committedEntriesArr, committedEntry)
			default:
				finished = true
			}
		}

		if len(committedEntriesArr) > 0 {
			kv.mu.Lock()
			for i := 0; i < len(committedEntriesArr); i++ {
				committedEntry := committedEntriesArr[i]

				committedOp := committedEntry.Command.(Op)
				commitKey := committedOp.Key
				commitName := committedOp.Name
				commitValue := committedOp.Value
				serialNum := committedOp.SerialNumID

				kv.committedCommands = append(kv.committedCommands, committedOp)

				_, clientResponsesExist := kv.clientResponses[serialNum]

				if !clientResponsesExist {
					if commitName == "Get" {
						reply := GetReply{}
						key_val, key_exists := kv.keyValuePairs[commitKey]
						if !key_exists {
							reply.Err = ErrNoKey
							reply.Value = ""
						} else {
							reply.Err = OK
							reply.Value = key_val
						}
						kv.clientResponses[serialNum] = reply
					} else if commitName == "Put" {
						reply := PutAppendReply{}
						kv.keyValuePairs[commitKey] = commitValue
						reply.Err = OK
						kv.clientResponses[serialNum] = reply
					} else {
						reply := PutAppendReply{}
						_, key_exists := kv.keyValuePairs[commitKey]
						reply.Err = OK

						if key_exists {
							kv.keyValuePairs[commitKey] += commitValue
						} else {
							kv.keyValuePairs[commitKey] = commitValue
						}
						kv.clientResponses[serialNum] = reply
					}
				}
			}
			kv.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.keyValuePairs = make(map[string]string)
	kv.executedSerialNum = -1
	kv.executedRequestNum = make(map[int]int)
	kv.executedGetRequestReply = make(map[int]string)
	kv.readCh = make(chan interface{})
	kv.clientreadCh = make(map[int]chan interface{})
	kv.executedStartOrder = make([]int, 0)
	kv.committedCommands = make([]Op, 1)
	kv.commitIndex = 0
	kv.nextCommitIndex = 1
	kv.clientOps = make(map[int]Op)

	kv.clientGetCh = make(map[int]chan GetReply)
	kv.clientPutAppendCh = make(map[int]chan PutAppendReply)
	kv.clientStartIndex = make(map[int]int)
	kv.clientResponses = make(map[int]interface{})
	kv.clientDuplicates = make(map[int]bool)

	kv.requestStartTime = make(map[int]time.Time)

	kv.timeout = time.Duration(30)
	kv.totalRunTime = time.Duration(0)
	kv.totalRunRequests = 0
	kv.commandIndex = 0

	//// 1. Ensure that there is a structure to keep track of the order in which requests is poked by transaction id and client id
	//// 2. Ensure that the channel will be closed for client after certain timeout

	// go kv.receiveStateMachineWrapper()
	go kv.applyStateMachine()

	return kv
}
