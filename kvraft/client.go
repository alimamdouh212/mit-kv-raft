package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"sync"
	"time"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	id          int
	tobeexcuted chan servrequestarg
	leader      int
	term        int
	i           int
	// You will have to modify this struct.
}
type findleaderchaarg struct {
	isleader  bool
	term      int
	commitind int
	result    string
}

type servrequestarg struct {
	reply     chan string
	operation Op
}
type Servercallargs struct {
	Operation Op
	Term      int
	Clintid   int
}
type Serverreply struct {
	Isleader bool
	Result   string
	Index    int
	Term     int
	Dupli    bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var clerkid int = 0
var mutex sync.Mutex

func getid() int {
	mutex.Lock()
	clerkid++
	mutex.Unlock()
	return clerkid
}
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = getid()
	ck.tobeexcuted = make(chan servrequestarg)
	ck.leader = 0
	ck.term = 0
	ck.i = 1
	// You'll have to add code here.
	go ck.excute()
	return ck
}

type leaderchanargs struct {
	result string
	ind    int
	leader int
	term   int
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	operation := Op{}
	operation.Clintid = ck.id
	operation.Instid = 2

	operation.Key = key

	cha := servrequestarg{}
	cha.operation = operation
	ch1 := make(chan string)
	cha.reply = ch1
	ck.tobeexcuted <- cha

	return <-ch1
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) excute() {
	time.Sleep(1000 * time.Millisecond)

	for {
		comm := <-ck.tobeexcuted

		operation := comm.operation

		st := ""
		switch operation.Instid {
		case 0:
			st = "append"
		case 1:
			st = "put"
		case 2:
			st = "get"
		}
		DPrintf("the clint %d will execute the in number %d "+st+" key "+operation.Key+" value "+operation.Value+"\n", ck.id, ck.i)
		operation.Clineindex = ck.i

		ch1 := make(chan leaderchanargs)
		candid := ck.leader

		for {
			go ck.sendtoleader(operation, ck.term, candid, ch1)
		label2:
			select {
			case re := <-ch1:
				if re.ind != -1 {
					if re.ind > ck.i {
						fmt.Printf("erroooooor \n")
					} else {
						if re.ind == ck.i && re.term >= ck.term {
							ck.term = re.term
							candid = re.leader
							comm.reply <- re.result
							ck.i += 1
							goto label
						}
					}
				} else if !(re.leader == candid && re.ind == ck.i) {
					goto label2
				}
			case <-time.After(200 * time.Millisecond):
			}
			candid = (candid + 1) % len(ck.servers)

		}
	label:
		ck.leader = candid
	}
}
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	operation := Op{}
	operation.Clintid = ck.id
	if op == "Put" {
		operation.Instid = 1
	} else {
		operation.Instid = 0
	}
	operation.Key = key
	operation.Value = value
	operation.Clintid = ck.id
	cha := servrequestarg{}
	cha.operation = operation
	ch1 := make(chan string)
	cha.reply = ch1
	ck.tobeexcuted <- cha

	<-ch1

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendtoleader(operation Op, term int, server int, ch chan leaderchanargs) {
	ret := leaderchanargs{}
	args := Servercallargs{}
	args.Clintid = ck.id

	args.Term = term
	args.Operation = operation

	reply := Serverreply{}
	DPrintf("the clint %d send to number %d inst %d\n", ck.id, server, operation.Clineindex)

	ok := ck.servers[server].Call("RaftKV.Applyclerk", args, &reply)

	if !ok || !reply.Isleader {
		ret.ind = -1
		ch <- ret
		ret.leader = server
		return
	}
	if ok && reply.Dupli {
		return
	}
	if reply.Index < operation.Clineindex {
		fmt.Printf("errroooooooooor %d to server %d return %d samaller than %d \n", ck.id, server, reply.Index, operation.Clineindex)
	}

	if reply.Index > operation.Clineindex {
		return
	}
	ret.ind = reply.Index
	ret.result = reply.Result
	ret.leader = server
	ret.term = reply.Term
	if ck.i == operation.Clineindex {
		DPrintf("the clint %d send to number %d succeded in ins%d \n", ck.id, server, ck.i)

		ch <- ret
	}

}
