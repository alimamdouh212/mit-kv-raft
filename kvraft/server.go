package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"

	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Clintid    int
	Clineindex int
	Instid     int
	Key        string
	Value      string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *Raft
	applyCh chan ApplyMsg

	maxraftstate int // snapshot if log grows this big
	state        int
	term         int
	lastop       map[int]int
	data         map[string]string
	pens         map[int]pender
	lasts        map[int]int
	lastresults  map[int]string
	lastappended map[int]int
	// Your definitions here.
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan ApplyMsg)
	kv.rf = Make(servers, me, persister, kv.applyCh)
	kv.rf.boos = kv
	kv.state = 0
	kv.term = 0
	kv.data = make(map[string]string)
	kv.pens = make(map[int]pender)
	kv.lasts = make(map[int]int)
	kv.lastresults = make(map[int]string)
	kv.lastappended = make(map[int]int)
	go kv.applyer()
	// You may need initialization code here.

	return kv
}

type pender struct {
	ch  chan string
	ind int
}

func (kv *RaftKV) applyer() {
	for {
		command := <-kv.applyCh

		Operation := command.Command.(Op)
		if Operation.Clintid != -1 {
			st := ""
			result := ""
			//re, ok := kv.data[Operation.Key]
			switch Operation.Instid {
			case 0:
				st = "append"
				kv.data[Operation.Key] += Operation.Value
			case 1:
				st = "put"
				kv.data[Operation.Key] = Operation.Value
			case 2:
				st = "get"
				result = kv.data[Operation.Key]
			}
			if kv.state == 2 {
				DPrintf("the value " + Operation.Key + " is now " + kv.data[Operation.Key] + "\n")
			}
			kv.mu.Lock()
			if kv.state == 0 {
				DPrintf("the server  %d will execute the in number %d "+st+" key "+Operation.Key+" value "+Operation.Value+" for clint %d\n", kv.me, Operation.Clineindex, Operation.Clintid)
			} else {
				DPrintf("the leader %d will execute the in number %d "+st+" key "+Operation.Key+" value "+Operation.Value+" for clint %d\n", kv.me, Operation.Clineindex, Operation.Clintid)
			}

			if Operation.Clineindex != kv.lasts[Operation.Clintid]+1 {
				fmt.Printf("erroooooor the server %d commited a clind %d index %d and his last %d \n", kv.me, Operation.Clintid, Operation.Clineindex, kv.lasts[Operation.Clintid])
			}
			kv.lasts[Operation.Clintid]++
			kv.lastresults[Operation.Clintid] = result
			pen, ok := kv.pens[Operation.Clintid]
			if ok && pen.ind > Operation.Clineindex {
				fmt.Printf("the server %d commited a clind %d index %d and his last pen %d \n", kv.me, Operation.Clintid, Operation.Clineindex, pen.ind)
				kv.mu.Unlock()
				return
			}

			if ok && pen.ind == Operation.Clineindex {
				ch1 := pen.ch
				//kv.lastresults[Operation.Clintid] = result
				delete(kv.pens, Operation.Clintid)
				kv.mu.Unlock()
				ch1 <- result
			} else {
				kv.mu.Unlock()
			}

		}
	}
}
func (kv *RaftKV) getlastindex(ind int) int {
	_, ok := kv.lasts[ind]
	if !ok {
		kv.lasts[ind] = 0
	}
	v, ok2 := kv.lastappended[ind]
	if !ok2 {
		kv.lastappended[ind] = 0
	}else if v==-1{
		kv.lastappended[ind]=kv.lasts[ind]
	}
	
	return kv.lastappended[ind]
}
func (kv *RaftKV) Applyclerk(args Servercallargs, reply *Serverreply) {

	kv.mu.Lock()
	reply.Dupli = false
	if kv.state == 2 && kv.term >= args.Term {
		reply.Isleader = true
		reply.Term = kv.term
		lastindex := kv.getlastindex(args.Clintid)
		fmt.Printf(" the leader %d recive clind %d index %d and his last ind %d \n", kv.me, args.Operation.Clintid, args.Operation.Clineindex, lastindex)
		pen, ok := kv.pens[args.Operation.Clintid]
		if (kv.lasts[args.Clintid] > args.Operation.Clineindex) || (ok && pen.ind >= args.Operation.Clineindex) {
			//reply.Index = kv.lasts[args.Clintid]
			reply.Dupli = true

			kv.mu.Unlock()
			return
		}
		if lastindex+1 < args.Operation.Clineindex {
			fmt.Printf("erroooooor  1 the server %d  a clind %d send %d and his last ind %d \n", kv.me, args.Operation.Clintid, args.Operation.Clineindex, lastindex)

			kv.mu.Unlock()
			return
		}

		if !ok && kv.lasts[args.Clintid] == args.Operation.Clineindex {
			reply.Index = lastindex
			reply.Result = kv.lastresults[args.Clintid]
			kv.mu.Unlock()
			return

		}

		if !ok || pen.ind < args.Operation.Clineindex {
			fmt.Printf("the golden server %d clind %d index %d\n", kv.me, args.Clintid, args.Operation.Clineindex)

			ch1 := make(chan string)
			peni := pender{}
			peni.ind = args.Operation.Clineindex
			peni.ch = ch1
			kv.pens[args.Operation.Clintid] = peni
			if args.Operation.Clineindex == kv.lastappended[args.Clintid]+1 {
				kv.rf.Start(args.Operation)
			}
			kv.mu.Unlock()

			reply.Result, ok = <-ch1
			if ok {
				reply.Index = args.Operation.Clineindex
				fmt.Printf("the leader %d succ finished clint %d index %d\n", kv.me, args.Clintid, args.Operation.Clineindex)
				return
			} else {
				reply.Isleader = false
				return
			}

		}
		fmt.Printf("erroooor unexpected case server %d clind %d \n", kv.me, args.Clintid)

	} else {
		reply.Isleader = false
		kv.mu.Unlock()
		return
	}

}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func (kv *RaftKV) befolllower(term int) {
	kv.mu.Lock()
	kv.term = term
	kv.state = 0
	for k, p := range kv.pens {
		close(p.ch)

		delete(kv.pens, k)
	}
	DPrintf("the server %d in now follower\n", kv.me)

	kv.mu.Unlock()
}
func (kv *RaftKV) beleader(term int) {
	kv.mu.Lock()
	kv.term = term
	kv.state = 2
	DPrintf("the server %d in now leader        %d %d \n", kv.me, len(kv.lasts), len(kv.lastappended))
	for k, v := range kv.lasts {
		DPrintf("the leader %d last commited for the clint %d is %d\n", kv.me, k, v)
	}
	for k, v := range kv.lastappended {
		DPrintf("the leader %d last appendedd for the clint %d is %d\n", kv.me, k, v)
	}
	kv.mu.Unlock()
}
func (kv *RaftKV) append(clintid int, insid int, locked bool) {
	if !locked {
		kv.mu.Lock()
		kv.lastappended[clintid] = insid
		kv.mu.Unlock()
	} else {
		kv.lastappended[clintid] = insid
		return
	}
}
