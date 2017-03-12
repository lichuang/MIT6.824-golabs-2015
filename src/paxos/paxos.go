package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*instance
	peerDones []int
}

type propose struct {
	num   string
	value interface{}
}

type instance struct {
	proposeId  int
	value      interface{}
	prepareNum string
	accepted   propose
	state      Fate
}

type request struct {
	pnum      string
	pvalue    interface{}
	reply     *PaxosReply
	peer      string
	proposeId int
	waitGroup *sync.WaitGroup
}

type prepareResult struct {
	decided bool
	propose propose
}

const enableDebug = false

func debug(format string, a ...interface{}) {
	if enableDebug {
		fmt.Printf(format+"\n", a...)
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// helper functions
func (px *Paxos) generateSeq() string {
	now := time.Now()
	return strconv.FormatInt(int64(now.Nanosecond()), 10) + "-" + strconv.Itoa(px.me)
}

func (px *Paxos) newInstance(proposeId int) *instance {
	instance := &instance{
		value:      nil,
		proposeId:  proposeId,
		prepareNum: "",
		accepted:   propose{num: "", value: nil},
		state:      Pending,
	}
	px.instances[proposeId] = instance
	return instance
}

func (px *Paxos) makeDecision(proposeId int, propose *propose) {
	instance, exist := px.instances[proposeId]
	if !exist {
		instance = px.newInstance(proposeId)
	}

	instance.accepted = *propose
	instance.state = Decided
}

func (px *Paxos) isMajority(num int) bool {
	return num > len(px.peers)/2
}

// RPC handler
func (px *Paxos) Prepare(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	proposeId := args.ProposeId
	pnum := args.PNum

	reply.Result = REJECT

	instance, exist := px.instances[proposeId]
	debug("exist = %v", exist)
	if !exist { // not exist
		instance = px.newInstance(proposeId)
		reply.Result = OK
	} else { // exist
		if instance.prepareNum < pnum {
			reply.Result = OK
		}
	}

	if reply.Result == OK {
		// if OK,return accept <PNum, PValue>
		reply.Pnum = instance.accepted.num
		reply.PValue = instance.accepted.value
		instance.prepareNum = pnum
	}

	debug("me=%s, args=%v, reply=%v", px.peers[px.me], args, reply)
	return nil
}

func (px *Paxos) Accept(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	proposeId := args.ProposeId
	pnum := args.PNum
	pvalue := args.PValue
	reply.Result = REJECT

	instance, exist := px.instances[proposeId]
	if !exist { // not exist
		instance = px.newInstance(proposeId)
	}

	if pnum >= instance.prepareNum {
		instance.accepted = propose{num: pnum, value: pvalue}
		instance.prepareNum = pnum
		reply.Result = OK
	}
	return nil
}

func (px *Paxos) Decision(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.makeDecision(args.ProposeId, &propose{num: args.PNum, value: args.PValue})
	px.peerDones[args.Me] = args.Done
	return nil
}

func (px *Paxos) doSendPrepare(request *request) {
	args := PaxosArgs{
		PNum:      request.pnum,
		PValue:    request.pvalue,
		Me:        px.me,
		ProposeId: request.proposeId,
	}

	if request.peer != px.peers[px.me] {
		call(request.peer, "Paxos.Prepare", &args, request.reply)
	} else {
		px.Prepare(&args, request.reply)
	}

	request.waitGroup.Done()
}

func (px *Paxos) doSendAccept(request *request) {
	args := PaxosArgs{
		PNum:      request.pnum,
		PValue:    request.pvalue,
		Me:        px.me,
		ProposeId: request.proposeId,
	}

	if request.peer != px.peers[px.me] {
		call(request.peer, "Paxos.Accept", &args, request.reply)
	} else {
		px.Accept(&args, request.reply)
	}

	request.waitGroup.Done()
}

func (px *Paxos) sendPrepare(seq int, v interface{}) *prepareResult {
	var waitGroup sync.WaitGroup
	var peerNum = len(px.peers)
	var requests []*request = make([]*request, peerNum)

	waitGroup.Add(len(px.peers))

	// send request to all peers
	pnum := px.generateSeq()
	for i, peer := range px.peers {

		reply := new(PaxosReply)

		request := &request{
			pnum:      pnum,
			pvalue:    v,
			reply:     reply,
			waitGroup: &waitGroup,
			proposeId: seq,
			peer:      peer,
		}
		requests[i] = request
		go px.doSendPrepare(request)
	}

	// wait for response
	waitGroup.Wait()

	//
	pvalue := v
	okNum := 0
	for _, request := range requests {
		reply := request.reply
		if reply.Result != OK {
			continue
		}

		okNum++
		if reply.Pnum > pnum {
			pvalue = reply.PValue
			pnum = reply.Pnum
		}
	}

	return &prepareResult{
		decided: px.isMajority(okNum),
		propose: propose{num: pnum, value: pvalue},
	}
}

func (px *Paxos) sendAccept(seq int, propose *propose) bool {
	var waitGroup sync.WaitGroup
	var peerNum = len(px.peers)
	var requests []*request = make([]*request, peerNum)
	var num = 0

	waitGroup.Add(peerNum)

	pnum := propose.num
	pvalue := propose.value

	for i, peer := range px.peers {
		reply := new(PaxosReply)

		request := &request{
			reply:     reply,
			pnum:      pnum,
			pvalue:    pvalue,
			waitGroup: &waitGroup,
			proposeId: seq,
			peer:      peer,
		}
		requests[i] = request
		go px.doSendAccept(request)
	}

	waitGroup.Wait()

	for _, request := range requests {
		if request.reply.Result == OK {
			num++
		}
	}

	return px.isMajority(num)
}

func (px *Paxos) sendDecision(seq int, propose *propose) {
	args := PaxosArgs{
		PNum:      propose.num,
		PValue:    propose.value,
		Me:        px.me,
		ProposeId: seq,
	}
	var reply PaxosReply

	for i, peer := range px.peers {
		if i != px.me {
			call(peer, "Paxos.Decision", &args, &reply)
		} else {
			px.Decision(&args, &reply)
		}
	}
}

func (px *Paxos) doPropose(seq int, v interface{}) {
	for {
		prepareResult := px.sendPrepare(seq, v)
		debug("prepare result: %v", prepareResult)
		if !prepareResult.decided {
			continue
		}

		ok := px.sendAccept(seq, &prepareResult.propose)
		if !ok {
			continue
		}

		px.sendDecision(seq, &prepareResult.propose)
		break
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		if seq < px.Min() {
			return
		}
		px.doPropose(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	me := px.me
	if px.peerDones[me] < seq {
		px.peerDones[me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	largest := 0
	for i := range px.instances {
		if i > largest {
			largest = i
		}
	}
	return largest
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	me := px.me
	min := px.peerDones[me]
	for i := range px.peerDones {
		if min > px.peerDones[i] {
			min = px.peerDones[i]
		}
	}

	for i := range px.instances {
		if i <= min && px.instances[i].state == Decided {
			delete(px.instances, i)
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	min := px.Min()
	if seq < min {
		return Pending, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.instances[seq]; !exist {
		return Forgotten, nil
	}
	return px.instances[seq].state, px.instances[seq].accepted.value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*instance)
	px.peerDones = make([]int, len(peers))
	for i := range peers {
		px.peerDones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
