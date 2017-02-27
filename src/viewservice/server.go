package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

// acknowledgment rule:the view service must not change views
// until the primary from the current view acknowledges that
// it is operating in the current view.
// 应答规则：只有在当前的primary应答了当前的view，才能修改view。
// 因此，假如primary没有应答当前的view，那么即使在primary过期的情况下，
// 也不允许切换到下一个状态

type serverState struct {
	acked uint
	tick  uint
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	current      View
	primaryState serverState
	backupState  serverState
	currentTick  uint
}

// helper functions
func (vs *ViewServer) acked() bool {
	return vs.primaryState.acked == vs.current.Viewnum
}

func (vs *ViewServer) hasPrimary() bool {
	return vs.current.Primary != ""
}

func (vs *ViewServer) isPrimary(name string) bool {
	return vs.current.Primary == name
}

func (vs *ViewServer) isBackup(name string) bool {
	return vs.current.Backup == name
}

func (vs *ViewServer) hasBackup() bool {
	return vs.current.Backup != ""
}

func (vs *ViewServer) isDead(serverState *serverState) bool {
	return (vs.currentTick-serverState.tick >= DeadPings)
}

func (vs *ViewServer) promoteBackup() {
	if !vs.hasBackup() {
		return
	}

	vs.current.Viewnum++
	vs.current.Primary = vs.current.Backup
	vs.current.Backup = ""
	vs.primaryState = vs.backupState
	vs.backupState = serverState{0, 0}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	name := args.Me
	viewNum := args.Viewnum

	vs.mu.Lock()

	switch {
	// no primary
	case !vs.hasPrimary() && vs.current.Viewnum == 0:
		vs.current.Primary = name
		vs.current.Viewnum++
		vs.primaryState.acked = 0
		vs.primaryState.tick = vs.currentTick

	// no backup and primary has acked
	case !vs.isPrimary(name) && !vs.hasBackup() && vs.acked():
		// 只有在ack的状态下才能修改view
		vs.current.Backup = name
		vs.current.Viewnum++
		vs.backupState.tick = vs.currentTick

	// primary ping
	case vs.isPrimary(name):
		if viewNum == 0 {
			// primary以ping 0启动，表示是从crash状态重启
			// 此时可以进行切换，将backup提升为primary
			vs.promoteBackup()
		} else {
			vs.primaryState.acked = viewNum
			vs.primaryState.tick = vs.currentTick
		}

	// backup ping
	case vs.isBackup(name):
		if viewNum == 0 && vs.acked() {
			// viewnum=0表示是刚启动
			// acked返回true表示当前primary应答了当前view
			// 这种情况下才允许修改view，成为backup
			vs.current.Backup = name
			vs.current.Viewnum++
			vs.backupState.tick = vs.currentTick
		} else if viewNum != 0 {
			vs.backupState.tick = vs.currentTick
			vs.backupState.acked = viewNum
		}
	}

	reply.View = vs.current

	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.current
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.currentTick++

	// 尽管primary超时，但是只要不满足ack条件，也不能将backup提升为primary
	if vs.isDead(&vs.primaryState) && vs.acked() {
		vs.promoteBackup()
	}

	if vs.isDead(&vs.backupState) && vs.hasBackup() && vs.acked() {
		vs.current.Backup = ""
		vs.current.Viewnum++
		vs.backupState = serverState{0, 0}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	// Your vs.* initializations here.
	vs.current = View{0, "", ""}
	vs.primaryState = serverState{0, 0}
	vs.backupState = serverState{0, 0}
	vs.currentTick = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
