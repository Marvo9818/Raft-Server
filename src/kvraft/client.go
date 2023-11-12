package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverID          int // The server that client is connected to
	commandSerialNum  int // The serial number that the command is assigned to
	leader            int // The recent leader that client has talked to
	executedSerialNum int // The serial number whose request is already executed
	clientID          int // Client ID
	recentLeaderID    int // The last time that client talks to
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//// MakeClerk: Accepting Get request,
//// Run in a loop until it hits success for any of the request
//// ApplyState

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	// When a client first starts up, it connects to a randomly-chosen server. If the client’s
	// first choice is not the leader, that server will reject the client’s request and supply
	// information about the most recent leader it has heard from (AppendEntries requests include
	// the network address of the leader). If the leader crashes, client requests will time
	// out; clients then try again with randomly-chosen servers.
	ck.serverID = int(nrand()) % (len(ck.servers))
	ck.commandSerialNum = int(nrand())
	ck.leader = -1
	ck.clientID = ck.commandSerialNum
	ck.recentLeaderID = -1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	result := ""

	args := GetArgs{}
	reply := GetReply{}

	args.Key = key
	args.SerialNum = ck.commandSerialNum
	args.ClientID = ck.clientID

	for {
		ok := ck.servers[ck.serverID].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				result = reply.Value
				ck.commandSerialNum++
				ck.recentLeaderID = ck.serverID
				break
			}

			if reply.Err == ErrNoKey {
				result = ""
				ck.commandSerialNum++
				ck.recentLeaderID = ck.serverID
				break
			}

			if reply.Err == ErrWrongLeader {
				ck.serverID = (int(nrand())) % (len(ck.servers))
			}
		} else {
			ck.serverID = (int(nrand())) % (len(ck.servers))
		}

		if ck.recentLeaderID > 0 {
			ok := ck.servers[ck.recentLeaderID].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					result = reply.Value
					ck.commandSerialNum++
					ck.serverID = ck.recentLeaderID
					break
				}
				if reply.Err == ErrNoKey {
					result = ""
					ck.commandSerialNum++
					ck.serverID = ck.recentLeaderID
					break
				}
			}
		}
	}

	return result
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// args := PutAppendArgs{}
	// reply := PutAppendReply{}

	// args.Key = key
	// args.Op = op
	// args.SerialNum = ck.commandSerialNum
	// args.Value = value

	args := PutAppendArgs{}
	reply := PutAppendReply{}

	args.Key = key
	args.Op = op
	args.SerialNum = ck.commandSerialNum
	args.Value = value
	args.ClientID = ck.clientID

	for {
		ok := ck.servers[ck.serverID].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.commandSerialNum++
				ck.recentLeaderID = ck.serverID
				break
			}

			if reply.Err == ErrNoKey {
				ck.commandSerialNum++
				ck.recentLeaderID = ck.serverID
				break
			}

			if reply.Err == ErrWrongLeader {
				ck.serverID = (int(nrand())) % (len(ck.servers))

			}
		} else {
			ck.serverID = (int(nrand())) % (len(ck.servers))
		}

		if ck.recentLeaderID > 0 {
			ok := ck.servers[ck.recentLeaderID].Call("KVServer.PutAppend", &args, &reply)

			if ok {
				if reply.Err == OK {
					ck.commandSerialNum++
					ck.serverID = ck.recentLeaderID
					break
				}

				if reply.Err == ErrNoKey {
					ck.commandSerialNum++
					ck.serverID = ck.recentLeaderID
					break
				}

			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
