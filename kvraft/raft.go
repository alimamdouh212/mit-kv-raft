package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type reuntu struct {
	isleader bool
	index    int
	term     int
}
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
type Logentry struct {
	Command interface{}
	Term    int
}
type warnmessage struct {
	term    int
	trafic  chan bool
	blocked bool
}

//
// A Go object implementing a single Raft peer.
//
type logmessreply struct {
	command interface{}
	reply   chan int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	stat_numper  int
	applyCh      chan ApplyMsg
	loges        map[int]Logentry
	term         int
	lastindex    int
	lastcommited int
	lastapply    int
	lastvot      int

	nextindex  []int
	commitchan chan int

	replylogchan    chan logmessreply
	warn            chan warnmessage
	firstleader     bool
	lasttermresived int
	boos            *RaftKV
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := false
	if rf.stat_numper == 2 {
		isleader = true
	}
	// Your code here (2A).
	return rf.term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.loges)
	e.Encode(rf.lastindex)
	e.Encode(rf.lastvot)
	e.Encode(rf.term)
	e.Encode(rf.lasttermresived)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//

func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.loges)
	d.Decode(&rf.lastindex)
	d.Decode(&rf.lastvot)
	d.Decode(&rf.term)
	//d.Decode(&rf.lastapply)
	//d.Decode(&rf.lastcommited)
	d.Decode(&rf.lasttermresived)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	Lasttermlog int
	Lastindex   int
	Me          int
	Pro         string
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok bool

	Term int
}
type LeadermessaeReply struct {
	// Your data here (2A).
	Ok               bool
	Wordone          bool
	Lastindexapplied int
	Faildmatch       int
	Term             int
}

//
// example RequestVote RPC handler.
//
type leadermessageArgs struct {
	Current_commit int
	Logs           map[int]Logentry
	Match          int
	Me             int
	Term           int
}

func (rf *Raft) Applyleader(args leadermessageArgs, reply *LeadermessaeReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.me == rf.lastvot+1 {
		//fmt.Printf("the %d reciev lwadermessa \n", rf.me)
	}
	reply.Ok = false
	reply.Term = rf.term
	reply.Wordone = false

	lastindex := rf.lastindex

	if rf.term > args.Term {
		return
	} else {
		reply.Ok = true

		/*if args.Current_commit > lasappl && lastindex >= args.Current_commit {
			rf.lastcommited = args.Current_commit
			rf.commitchan <- args.Current_commit
		}*/

		if rf.term < args.Term {
			//fmt.Printf("the server %d with stat %d raised his term from %d to %d \n", rf.me, rf.stat_numper, rf.term, args.Term)
			rf.stat_numper = 0
			//rf.term = args.Term

		}
		/*if args.Term == rf.term && (rf.lastvot != args.Me && rf.lastvot != -1) {
			reply.Faildmatch = -15
			return
		}*/
		warner := warnmessage{}
		traffic := make(chan bool)
		warner.trafic = traffic
		warner.term = args.Term
		warner.blocked = true
		rf.warn <- warner
		<-traffic
		if args.Match+len(args.Logs)-1 < rf.lastindex && rf.lasttermresived == args.Term {
			reply.Lastindexapplied = rf.lastindex
			reply.Wordone = true
			return

		}
		if len(args.Logs) != 0 {
			reply.Wordone = true
			lasttobepplied := len(args.Logs) + args.Match - 1
			//fmt.Printf("the %d took logs meesage the match %d the lastindex %d \n", rf.me, args.Match, lasttobepplied)
			if args.Match > lastindex {
				//fmt.Printf("failed %d took logs meesage the match %d the lastindex %d \n", rf.me, args.Match, lasttobepplied)
				reply.Faildmatch = args.Match
				reply.Wordone = false
				return
			}

			entry1 := args.Logs[args.Match]
			entry2 := rf.loges[args.Match]

			if entry1.Term == entry2.Term {
				in := (args.Match + 1)
				i := 0
				re := false

				inn := in + len(args.Logs) - 1
				laslas := rf.lastindex
				for inn <= rf.lastindex {
					re = true
					op := rf.loges[inn].Command.(Op)
					fmt.Printf("removed the index %d    from clind %d in server %d under leader %d  \n ", op.Clineindex, op.Clintid, rf.me, args.Me)
					rf.boos.append(op.Clintid, -1, false)
					inn++
				}
				for i < len(args.Logs)-1 {
					rf.lastindex = in + i
					entry := Logentry{}
					entry.Command = args.Logs[in+i].Command
					entry.Term = args.Logs[in+i].Term
					op := entry.Command.(Op)

					if rf.loges[in+i].Term != entry.Term && laslas >= rf.lastindex {
						re = true
						rf.boos.append(op.Clintid, -1, false)

						fmt.Printf("removed the index %d  from clind %d in server %d under leader %d te1 %d t2 %d ind1 %d ind2 %d \n ", op.Clineindex, op.Clintid, rf.me, args.Me, rf.loges[in+i].Term, entry.Term, rf.loges[in+i].Command.(Op).Clineindex, op.Clineindex)
					}

					rf.loges[in+i] = entry
					fmt.Printf("the index %d in clint %d appended in server %d from leader %d with term %d \n ", op.Clineindex, op.Clintid, rf.me, args.Me, entry.Term)

					rf.boos.append(op.Clintid, op.Clineindex, false)
					//fmt.Printf("the %d  state %d withindex %d lastappend %d commited for %d index %d\n", rf.me, rf.stat_numper, appm.Index, rf.lastcommited, op.Clintid, op.Clineindex)
					i++

				}
				if re {
					fmt.Printf("the match bettwen %d and %d was %d \n", rf.me, args.Me, entry1.Term)
				}
				if len(args.Logs) == 1 {
					if args.Current_commit >= args.Match && args.Match > rf.lastcommited {
						rf.lastcommited = args.Match
					}
				} else {
					if args.Current_commit > rf.lastcommited && args.Current_commit <= rf.lastindex {
						rf.lastcommited = args.Current_commit
					}
				}
				if rf.lastcommited > rf.lastapply && rf.lastcommited <= rf.lastindex {
					rf.commitchan <- rf.lastcommited
				}
				reply.Wordone = true
				reply.Lastindexapplied = lasttobepplied
				rf.lastvot = args.Me
				if len(args.Logs) > 1 {
					rf.lasttermresived = args.Term
				}
				(*rf).persist()
				//fmt.Printf("succ %d took logs meesage the match %d the lastindex %d \n", rf.me, args.Match, lasttobepplied)

			} else {
				//fmt.Printf("failed %d took logs meesage the match %d the lastindex %d \n", rf.me, args.Match, lasttobepplied)
				reply.Faildmatch = args.Match
				reply.Wordone = false
			}

		}
	}

}

func (rf *Raft) timer(dur int, wake *chan bool) {
	time.Sleep(time.Duration(dur) * time.Millisecond)
	(*wake) <- true
}
func (rf *Raft) randtimer(dur int, wake *chan bool, base int) {
	dur = rand.Intn(dur) + base
	time.Sleep(time.Duration(dur) * time.Millisecond)
	(*wake) <- true
}
func (rf *Raft) sendorders(term int, server int, waker *(chan LeadermessaeReply), index int, logs *(map[int]Logentry)) {
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	args := leadermessageArgs{}
	args.Current_commit = rf.lastcommited
	args.Match = index

	args.Me = rf.me

	args.Logs = *logs
	args.Term = term
	reply := LeadermessaeReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.Applyleader", args, &reply)
	if ok && reply.Ok {
		rf.mu.Lock()

		if reply.Lastindexapplied > rf.nextindex[server] {
			rf.nextindex[server] = reply.Lastindexapplied
			//fmt.Printf("leader %d succ %d took logs meesage the match %d the lastindex %d \n", rf.me, server, args.Match, reply.Lastindexapplied)
			rf.mu.Unlock()
			(*waker) <- reply
		} else if reply.Wordone == false {
			//fmt.Printf("fail %d took logs meesage the match %d the lastindex %d \n", server, args.Match, reply.Lastindexapplied)
			rf.mu.Unlock()
			(*waker) <- reply
		} else {
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) AppendEntrie(term int, beer int, entry Logentry, preventry Logentry, lastindex int, link *(chan int)) {

	logs := make(map[int]Logentry)
	logs[lastindex] = entry
	firsindex := lastindex - 1
	logs[firsindex] = preventry
	lastindexapplied := -1
	//timerchan:=make(chan bool)
	replychan := make(chan LeadermessaeReply)
	for {

		lastfailed := firsindex
		back := false
		Logs := make(map[int]Logentry)
		for in, v := range logs {
			Logs[in] = v
		}
		//fmt.Printf("the leader %d send to %d order match %d last index %d \n", rf.me, beer, firsindex, lastindex)
		go (*rf).sendorders(term, beer, &replychan, firsindex, &logs)
		select {
		case <-time.After(100 * time.Millisecond):

		case myreply := <-replychan:
			if myreply.Wordone == false {

				back = true
				if myreply.Faildmatch < lastfailed {
					lastfailed = myreply.Faildmatch
				}
			} else {
				if myreply.Lastindexapplied > lastindexapplied {
					lastindexapplied = myreply.Lastindexapplied
				}
			}

		}
		if lastindexapplied < lastindex {

			select {
			case ok := <-(*link):
				if ok == -1 {
					return
				} else {
					rf.mu.Lock()
					for lastindex < ok {
						lastindex++
						logs[lastindex] = rf.loges[lastindex]

					}
					rf.mu.Unlock()

				}
			default:
			}
			if back {
				rf.mu.Lock()
				if lastfailed != -1 {
					//fmt.Printf("abdelnaser server %d with %d is  %d firstindex %d lastindexapplies %d \n", rf.me, beer, lastfailed, firsindex, lastindexapplied)
				}
				if lastfailed <= firsindex && lastfailed > lastindexapplied {
					firsindex--
					logs[firsindex] = rf.loges[firsindex]

				}
				rf.nextindex[beer] = firsindex
				rf.mu.Unlock()
			}
		} else {

			rf.mu.Lock()
			(*rf).commitdec()

			sameold := true
			for firsindex < lastindexapplied {
				delete(logs, firsindex)
				firsindex++
			}
			rf.nextindex[beer] = firsindex
			rf.mu.Unlock()
			for sameold {
				//fmt.Printf("%d will sleep after  %d lkberr index %d\n", beer, lastindex, rf.lastindex)

				ok := <-(*link)

				if ok == -1 {
					return
				} else {

					rf.mu.Lock()
					//fmt.Printf("the server %d  with state %d woke up for %d lastindex   %d\n", rf.me, rf.stat_numper, beer, rf.lastindex)
					for lastindex < ok {
						sameold = false
						lastindex++
						logs[lastindex] = rf.loges[lastindex]
					}
					rf.mu.Unlock()

				}
			}
		}

	}

}
func (rf *Raft) commiter() {

	for {
		commendindex := <-rf.commitchan
		//stat := rf.stat_numper
		//fmt.Print(stat)
		for rf.lastapply < commendindex {
			appm := ApplyMsg{}
			rf.mu.Lock()
			appm.Command = rf.loges[rf.lastapply+1].Command
			rf.mu.Unlock()
			appm.Index = rf.lastapply + 1
			rf.applyCh <- appm
			op := appm.Command.(Op)
			fmt.Printf("the %d  state %d withindex %d lastcommites %d commited for %d index %d\n", rf.me, rf.stat_numper, appm.Index, rf.lastcommited, op.Clintid, op.Clineindex)
			//fmt.Println(appm.Command)

			rf.lastapply++
		}

	}
}
func (rf *Raft) commitdec() {
	arr := []int{}
	for _, laslog := range rf.nextindex {

		arr = append(arr, laslog)

	}
	sort.Ints(arr)
	num := len(arr)
	num = num / 2
	criticalindex := arr[num]

	term1 := rf.loges[criticalindex].Term
	term2 := rf.term
	lastcommited := rf.lastcommited
	if (criticalindex > lastcommited && term1 == term2) || (arr[0] > lastcommited) {
		rf.lastcommited = criticalindex
		//fmt.Printf("leader %d new commit %d \n", rf.me, rf.lastcommited)
		rf.commitchan <- rf.lastcommited
	} else {
		//fmt.Printf("the %d not now \n", rf.me)
	}

}
func (rf *Raft) senhartbeat(beer int) {
	if beer == rf.me+1 {
		//fmt.Printf("the %d senheart to %d\n", rf.me, beer)
	}
	args := leadermessageArgs{}
	args.Logs = make(map[int]Logentry)
	args.Me = rf.me
	rf.mu.Lock()
	args.Term = rf.term
	args.Current_commit = rf.lastcommited
	args.Logs[rf.nextindex[beer]] = rf.loges[rf.nextindex[beer]]
	args.Match = rf.nextindex[beer]
	rf.mu.Unlock()
	reply := LeadermessaeReply{}
	//fmt.Printf("the leader %d send heart beat to %d \n", rf.me, beer)
	ok := rf.peers[beer].Call("Raft.Applyleader", args, &reply)
	if ok && !reply.Ok {
		//fmt.Printf("fail the leader %d send heart beat to %d \n", rf.me, beer)
		raise := false
		rf.mu.Lock()
		if rf.term < reply.Term {
			rf.stat_numper = 0
			//rf.term = reply.Term
			raise = true
		}
		rf.mu.Unlock()
		if raise {
			warrner := warnmessage{}
			warrner.blocked = true
			warrner.term = reply.Term
			traffice := make(chan bool)
			warrner.trafic = traffice
			rf.mu.Lock()
			rf.warn <- warrner
			<-warrner.trafic
			rf.mu.Unlock()
		}
	} else {
		//fmt.Printf("succ the leader %d send heart beat to %d \n", rf.me, beer)
	}
}
func (rf *Raft) heartbeater(beer int, rope *(chan bool)) {
	for {
		select {
		case <-time.After(100 * time.Millisecond):

			go rf.senhartbeat(beer)
		case <-*(rope):
			return

		}
	}

}

/*func (rf *Raft) decied() {
	fmt.Printf("the %d decied \n", rf.me)
	rf.mu.Lock()
	if rf.stat_numper == 2 {
		fmt.Printf("the %d decied leader \n", rf.me)
		(*rf).leadermain()
	} else {
		fmt.Printf("the %d decied follower \n", rf.me)
		rf.followermain()
	}
	rf.mu.Unlock()
}*/
func (rf *Raft) candidmain(term int) {
	//defer (*rf).decied()
	//fmt.Printf("the %d will electing with term%d \n", rf.me, rf.term)
	rf.mu.Lock()

	rf.lastvot = rf.me

	lastinde := rf.lastindex
	lastlogterm := (*rf).getLastTerm()
	rf.persist()
	rf.mu.Unlock()
	result := false
	for {
		//fmt.Printf("the %d retry electing \n", rf.me)

		win := make(chan bool)
		go (*rf).gathervotes(&win, term, lastlogterm, lastinde)
		select {

		case <-time.After(time.Duration(1000+rand.Intn(1000)) * time.Millisecond):

		case <-win:
			rf.mu.Lock()
			rf.stat_numper = 2
			result = true
			rf.mu.Unlock()
			goto label
		case warrner := <-rf.warn:
			if !warrner.blocked {
				rf.mu.Lock()
				rf.term = warrner.term
				//fmt.Printf("the server %d will stop candi eith term %d \n", rf.me, rf.term)

				rf.stat_numper = 0
				(*rf).persist()
				result = false
				rf.mu.Unlock()
			} else {
				if warrner.term > rf.term {
					rf.term = warrner.term
				}
				warrner.trafic <- true

			}
			goto label

		}
		rf.mu.Lock()

		rf.lastvot = rf.me
		rf.term++
		term = rf.term

		rf.persist()
		rf.mu.Unlock()
	}
label:

	if result == true {
		//fmt.Printf("the %d decied leader \n", rf.me)
		(*rf).leadermain(term)
	} else {
		//fmt.Printf("the %d decied follower \n", rf.me)
		rf.followermain()
	}

}
func (rf *Raft) followermain() {

	if rf.lastvot == rf.me {
		rf.lastvot = -1
	}
	i := 0
	//fmt.Printf("the %d now follower with term %d\n", rf.me, rf.term)
	//start := time.Now()
	term := -17
	for {
		i++
		var n int
		n = rand.Intn(500) + 500
		select {

		case <-time.After(time.Duration(n) * time.Millisecond):
			rf.mu.Lock()

			rf.stat_numper = 1
			rf.term++
			term = rf.term
			//fmt.Printf("the %d will electhimself %d with term %d %s\n", rf.me, i, term, time.Since(start))
			rf.persist()
			rf.mu.Unlock()
			goto label
		case warner := <-rf.warn:
			dec := warner.blocked
			if dec == false {
				rf.mu.Lock()
				if rf.term < warner.term {
					rf.term = warner.term
					(*rf).persist()
				}
				rf.mu.Unlock()
			} else {
				if rf.term < warner.term {
					rf.term = warner.term
					(*rf).persist()
					warner.trafic <- true
				} else {
					warner.trafic <- true
				}
			}

			//fmt.Printf("the %d will stayfollower %d %s\n", rf.me, i, time.Since(start))
			//fmt.Println()

		}
	}
label:
	(*rf).candidmain(term)

}
func (rf *Raft) leadermain(term int) {

	//lastindexterm := rf.loges[rf.lastindex].Term
	//lastlognumper := rf.loges[rf.lastindex].Command.(int)
	rf.boos.beleader(term)
	fmt.Printf("my bros %d\n", len(rf.peers))
	//fmt.Printf("the %d leader withlastterm %d withlastindex %d lastindexterm %d lastnumper %d \n", rf.me, term, rf.lastindex, lastindexterm, lastlognumper)
	if rf.term == 1 {
		rf.firstleader = true
	}
	//deliver := make(chan bool, 10)
	heartropes := make([](chan bool), len(rf.peers))
	logsropes := make([](chan int), len(rf.peers))
	rf.mu.Lock()
	lastindex := rf.lastindex
	rf.mu.Unlock()
	i := 0
	for i < len(rf.peers) {
		if i != rf.me {
			heartropes[i] = make((chan bool))
			logsropes[i] = make((chan int), 100)

			go (*rf).heartbeater(i, &heartropes[i])
			if lastindex != 0 {
				go (*rf).AppendEntrie(term, i, rf.loges[lastindex], rf.loges[lastindex-1], lastindex, &logsropes[i])
			}
		}
		i++
	}
	rf.mu.Lock()
	rf.lastindex++
	newlog := Logentry{}
	newcommand := Op{}
	newcommand.Clineindex = -1
	newlog.Command = newcommand
	newlog.Term = rf.term
	rf.loges[rf.lastindex] = newlog
	rf.nextindex[rf.me] = rf.lastindex
	if rf.lastindex == 1 {
		in := 0
		leng := len(rf.peers)
		for in < leng {
			if in != rf.me {

				go (*rf).AppendEntrie(term, in, rf.loges[rf.lastindex], rf.loges[rf.lastindex-1], rf.lastindex, &logsropes[in])
			}
			in++
		}

	}
	in := 0
	leng := len(rf.peers)
	for in < leng {
		if in != rf.me {

			select {
			case <-logsropes[in]:
			default:
			}
			logsropes[in] <- rf.lastindex
		}
		in++
	}
	(*rf).persist()
	rf.mu.Unlock()
	for {

		select {
		case warner := <-rf.warn:
			i := 0
			if !warner.blocked {
				rf.mu.Lock()
				if warner.term > rf.term {
					for i < len(rf.peers) {
						if i != rf.me {
							heartropes[i] <- false
							logsropes[i] <- -1
						}
						i++

					}
					rf.term = warner.term

					//fmt.Printf("the server %d will stop leader eith term %d \n", rf.me, rf.term)

					rf.stat_numper = 0
					(*rf).persist()
					rf.mu.Unlock()

					goto label
				} else {
					rf.mu.Unlock()

				}

			} else {
				if warner.term > rf.term {
					rf.boos.befolllower(warner.term)
					for i < len(rf.peers) {
						if i != rf.me {
							heartropes[i] <- false
							logsropes[i] <- -1
						}
						i++

					}
					rf.term = warner.term
					//fmt.Printf("the server %d will stop leader eith term %d \n", rf.me, rf.term)

					rf.stat_numper = 0
					(*rf).persist()
					warner.trafic <- true
					goto label
				} else {
					warner.trafic <- true
				}

			}
		/*case <-deliver:
		rf.mu.Lock()
		(*rf).commitdec()
		rf.mu.Unlock()*/
		case reply := <-rf.replylogchan:
			ch := reply.reply
			newcommand := reply.command

			if rf.stat_numper == 2 {
				//fmt.Printf("the server %d will increase becaus %d from %d \n", rf.me, newcommand.(int), rf.lastindex)
				rf.lastindex++
				newlog := Logentry{}
				newlog.Command = newcommand
				newlog.Term = rf.term
				rf.loges[rf.lastindex] = newlog
				rf.nextindex[rf.me] = rf.lastindex
				op := rf.loges[rf.lastindex].Command.(Op)
				rf.boos.append(op.Clintid, op.Clineindex, true)
				fmt.Printf("the leader %d appende ind %d of clint %d with term %d \n ", rf.me, op.Clineindex, op.Clintid, rf.term)
				if rf.lastindex == 1 {
					in := 0
					leng := len(rf.peers)
					for in < leng {
						if in != rf.me {

							go (*rf).AppendEntrie(term, in, rf.loges[rf.lastindex], rf.loges[rf.lastindex-1], rf.lastindex, &logsropes[in])
						}
						in++
					}

				}
				in := 0
				leng := len(rf.peers)
				for in < leng {
					if in != rf.me {

						select {
						case <-logsropes[in]:
						default:
						}
						logsropes[in] <- rf.lastindex
					}
					in++
				}
				(*rf).persist()

				ch <- rf.lastindex

			} else {
				ch <- -1
			}

		}

	}
label:
	(*rf).followermain()

}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer (*rf).persist()
	reply.Ok = false
	reply.Term = rf.term
	//fmt.Printf("the %d receve vote request from %d his term %d and the candi term %d  \n", rf.me, args.Me, rf.term, reply.Term)
	candterm := args.Term

	candindex := args.Lastindex
	candlastlogterm := args.Lasttermlog
	candid := args.Me

	if rf.term < candterm {
		rf.stat_numper = 0
		warner := warnmessage{}
		trafic := make(chan bool)
		warner.blocked = true
		warner.trafic = trafic
		warner.term = candterm
		rf.warn <- warner
		<-trafic
		//rf.term = candterm
		rf.lastvot = -1
	}

	laslog := rf.loges[rf.lastindex]
	lastlogterm := laslog.Term
	lastlogindex := rf.lastindex

	if lastlogterm <= candlastlogterm {
		if lastlogterm < candlastlogterm || lastlogindex <= candindex {
			if (rf.lastvot == -1) || (rf.lastvot == args.Me) {

				reply.Ok = true
				rf.lastvot = candid
				//fmt.Printf("the %d voted %d\n", rf.me, candid)

			}
		}

	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, votes *(chan bool)) bool {
	args.Pro = "dkms"
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok && reply.Ok == true {

		(*votes) <- true
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) gathervotes(win *(chan bool), term int, lastlogterm int, lastindex int) {
	var args RequestVoteArgs
	args.Lastindex = lastindex
	args.Lasttermlog = lastlogterm
	args.Me = rf.me
	args.Term = term
	i := 0
	votes := make(chan bool, len(rf.peers)-1)
	for i < len(rf.peers) {
		if i != rf.me {
			reply := RequestVoteReply{}
			go (*rf).sendRequestVote(i, args, &reply, &votes)

		}
		i++
	}
	votncout := 1
	for i := range votes {
		if !i {
			fmt.Println()
		}
		votncout++
		if votncout > len(rf.peers)/2 {
			(*win) <- true
		}
	}
}
func (rf *Raft) getLastIndex() int {
	return rf.lastindex
}
func (rf *Raft) getLastTerm() int {
	return rf.loges[rf.lastindex].Term
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	stat := rf.stat_numper

	//in := command.(int)

	//returned := reuntu{}

	if stat == 2 {
		//fmt.Printf("start %d try increase %d because %d \n", rf.me, rf.lastindex, in)
		reply := make(chan int)
		message := logmessreply{}

		//in *= (rf.lastindex + 1)
		message.command = command
		message.reply = reply

		rf.replylogchan <- message
		index := <-reply
		if index != -1 {
			//fmt.Printf("start %d succ got index %d because %d \n", rf.me, index, in)
			/*returned.term = rf.term
			returned.isleader = true
			returned.index = index*/
			return index, rf.term, true
		} else {
			//fmt.Printf("start %d fail increase %d because %d \n", rf.me, rf.lastindex, in)
			return -1, -1, false
			/*returned.term = -1
			returned.isleader = false
			returned.index = -1*/
		}
	} else {

		return -1, -1, false
		/*returned.term = -1
		returned.isleader = false
		returned.index = -1*/
	}
	/*fmt.Printf("we %d returned %d \n", rf.me, returned.index)
	retch <- returned*/

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//fmt.Printf("the %d dead \n", rf.me)

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.stat_numper = 0
	rf.applyCh = applyCh
	rf.loges = make(map[int]Logentry)
	logi := Logentry{}
	logi.Term = 0
	logi.Command = -1
	rf.loges[0] = logi
	rf.term = 0
	rf.lastindex = 0
	rf.lastapply = 0
	rf.lastcommited = 0
	rf.lastvot = -1
	rf.firstleader = false
	rf.commitchan = make(chan int, 10)
	rf.replylogchan = make(chan logmessreply)
	rf.warn = make(chan warnmessage)
	rf.lasttermresived = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextindex = make([]int, len(peers))
	in := 0
	for in < len(peers) {
		rf.nextindex[in] = 0
		in++
	}

	go (*rf).commiter()
	go (*rf).followermain()

	return rf
}
