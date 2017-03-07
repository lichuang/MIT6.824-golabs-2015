package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	data map[string]string
	seen map[int64]bool
	view viewservice.View
}

const Debug = false

func enableDebug() bool {
	return Debug
}

func debug(format string, a ...interface{}) {
	if enableDebug() {
		fmt.Printf(format+"\n", a...)
	}
}

func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isDuplicateSeq(seq int64) bool {
	_, ok := pb.seen[seq]
	if ok {
		return true
	}
	pb.seen[seq] = true

	return false
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	value, ok := pb.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}

	reply.Err = OK
	reply.Value = value

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	debug("me: %s, op: %s, %v:%s:%s", pb.me, args.Op, args.Seq, args.Key, args.Value)

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	reply.Err = OK
	if pb.isDuplicateSeq(args.Seq) {
		return nil
	}
	var value string
	if args.Op == Put {
		value = args.Value
	} else {
		oldValue, ok := pb.data[args.Key]
		if !ok {
			oldValue = ""
		}
		value = oldValue + args.Value
	}
	pb.data[args.Key] = value

	// sync update result to backup
	if pb.view.Primary == pb.me && pb.view.Backup != "" {
		syncArgs := &SyncUpdateArgs{Key: args.Key, Value: value, Seq: args.Seq}
		var syncReply SyncUpdateReply
		call(pb.view.Backup, "PBServer.SyncUpdate", syncArgs, &syncReply)
	}

	return nil
}

func (pb *PBServer) SyncUpdate(args *SyncUpdateArgs, reply *SyncUpdateReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	_, ok := pb.seen[args.Seq]
	if ok {
		return nil
	}

	pb.seen[args.Seq] = true
	pb.data[args.Key] = args.Value

	return nil
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.data = args.Data
	pb.seen = args.Seen

	if enableDebug() {
		for key, value := range pb.data {
			debug("sync %s %s:%s", pb.me, key, value)
		}
	}
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {

	}
	if view.Viewnum != pb.view.Viewnum {
		// view changed
		if view.Primary == pb.me && view.Backup != pb.view.Backup && view.Backup != "" {
			// update all data && seq to backup
			args := &SyncArgs{Data: pb.data, Seen: pb.seen}
			var reply SyncReply
			call(view.Backup, "PBServer.Sync", args, &reply)
		}
	}
	pb.view = view
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.seen = make(map[int64]bool)
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
