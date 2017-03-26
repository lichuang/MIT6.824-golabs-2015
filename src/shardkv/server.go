package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT       = "Put"
	APPEND    = "Append"
	GET       = "Get"
	RECONFIG  = "Reconfig"
	GROUPSYNC = "GroupSync"
)

type MergeResult struct {
	Data map[string]string // k -> v
	Seen map[string]int64  // client -> seq
}

type Op struct {
	// Your definitions here.
	Type        string
	Key         string
	Value       string
	UUID        int64
	Seq         int64
	Client      string
	Config      *shardmaster.Config // reconfig config number
	MergeResult *MergeResult
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	data      map[string]string // k -> v
	seen      map[string]int64  // client -> seq
	processed int               // paxos seq
	config    shardmaster.Config
}

func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) applyGet(op Op) {
	kv.seen[op.Client] = op.Seq
}

func (kv *ShardKV) applyPut(op Op) {
	kv.seen[op.Client] = op.Seq
	kv.data[op.Key] = op.Value

	DPrintf("%d put %s:%s", kv.me, op.Key, op.Value)
}

func (kv *ShardKV) applyAppend(op Op) {
	kv.seen[op.Client] = op.Seq
	old, exist := kv.data[op.Key]
	var newValue string
	if !exist {
		newValue = op.Value
	} else {
		newValue = old + op.Value
	}

	kv.data[op.Key] = newValue
}

func (kv *ShardKV) applyReconfig(op Op) {
	for k, v := range op.MergeResult.Data {
		kv.data[k] = v
	}

	for client, newSeq := range op.MergeResult.Seen {
		oldSeq, exist := kv.seen[client]
		if !exist || oldSeq < newSeq {
			kv.seen[client] = newSeq
		}
	}

	kv.config.Num = op.Config.Num
	//kv.config.Shards = make([]int64, shardmaster.NShards)
	for i, shard := range op.Config.Shards {
		kv.config.Shards[i] = shard
	}

	kv.config.Groups = map[int64][]string{}
	for gid, servers := range op.Config.Groups {
		gidServers := make([]string, 0)
		for _, server := range servers {
			gidServers = append(gidServers, server)
		}
		kv.config.Groups[gid] = gidServers
	}
}

func (kv *ShardKV) apply(op Op) {
	switch op.Type {
	case GET:
		kv.applyGet(op)
	case PUT:
		kv.applyPut(op)
	case APPEND:
		kv.applyAppend(op)
	case RECONFIG:
		kv.applyReconfig(op)
	case GROUPSYNC:
		// nothing to do?
	}
	kv.processed += 1
	kv.px.Done(kv.processed)
}

func (kv *ShardKV) checkOp(op Op) Err {
	switch op.Type {
	case RECONFIG:
		if kv.config.Num >= op.Config.Num {
			return OK
		}
	case PUT, GET:
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return ErrWrongGroup
		}

		seq, exist := kv.seen[op.Client]
		if exist && op.Seq <= seq {
			return OK
		}
	}

	return ""
}

func (kv *ShardKV) process(op Op) Err {
	var ok = false
	op.UUID = nrand()
	for !ok {
		err := kv.checkOp(op)
		if err != "" {
			return err
		}

		seq := kv.processed + 1
		status, t := kv.px.Status(seq)
		var res Op
		if status == paxos.Decided {
			res = t.(Op)
		} else {
			kv.px.Start(seq, op)
			res = kv.wait(seq)
		}

		//DPrintf("done %s <%s, %s> %d", res.Type, res.Key, res.Value, res.UUID)
		ok = res.UUID == op.UUID
		kv.apply(res)
	}

	return OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Type:   GET,
		Key:    args.Key,
		Client: args.Me,
		Seq:    args.Seq,
	}

	reply.Err = kv.process(op)
	if reply.Err != OK {
		return nil
	}

	value, exist := kv.data[args.Key]
	if !exist {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value
	}
	DPrintf("%d get %s:%s", kv.me, args.Key, reply.Value)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Type:   args.Op,
		Key:    args.Key,
		Client: args.Me,
		Value:  args.Value,
		Seq:    args.Seq,
	}

	reply.Err = kv.process(op)

	DPrintf("%d %s %s:%s %s", kv.me, args.Op, args.Key, args.Value, reply.Err)
	return nil
}

func (kv *ShardKV) GroupSync(args *GroupSyncArgs, reply *GroupSyncReply) error {
	config := kv.config
	shard := args.Shard
	if config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Type: GROUPSYNC,
	}
	kv.process(op)

	reply.Data = make(map[string]string)
	reply.Seen = make(map[string]int64)
	reply.Err = OK

	for key, value := range kv.data {
		if key2shard(key) != shard {
			continue
		}
		DPrintf("GroupSync %s:%s", key, value)
		reply.Data[key] = value
	}

	for client, seq := range kv.seen {
		reply.Seen[client] = seq
	}

	return nil
}

func (kv *ShardKV) merge(reply *GroupSyncReply, merge *MergeResult) {
	for k, v := range reply.Data {
		merge.Data[k] = v
	}

	for client, newSeq := range reply.Seen {
		seq, exists := merge.Seen[client]
		if !exists || seq < newSeq {
			merge.Seen[client] = newSeq
		}
	}
}

func (kv *ShardKV) reconfig(newConfig *shardmaster.Config) bool {
	//DPrintf("reconfig %v", newConfig)

	mergeResult := MergeResult{
		Data: make(map[string]string),
		Seen: make(map[string]int64),
	}
	oldConfig := &kv.config
	myGid := kv.gid

	for i := 0; i < shardmaster.NShards; i++ {
		oldGid := oldConfig.Shards[i]
		newGid := newConfig.Shards[i]

		if myGid == oldGid {
			continue
		}

		if myGid != newGid {
			continue
		}

		args := GroupSyncArgs{
			Shard:     i,
			ConfigNum: oldConfig.Num,
		}

		var reply GroupSyncReply
		for _, server := range oldConfig.Groups[oldGid] {
			ok := call(server, "ShardKV.GroupSync", &args, &reply)

			if !ok {
				continue
			}

			if reply.Err == OK {
				break
			}
			if reply.Err == ErrNotReady {
				return false
			}
		}

		// ok, now merge data
		kv.merge(&reply, &mergeResult)
	}

	op := Op{
		Type:        RECONFIG,
		Config:      newConfig,
		MergeResult: &mergeResult,
	}

	//DPrintf("do merge %v", mergeResult)
	kv.process(op)

	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)
	oldConfig := kv.config
	for i := oldConfig.Num + 1; i <= newConfig.Num; i++ {
		cfg := kv.sm.Query(i)
		if !kv.reconfig(&cfg) {
			return
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{Num: -1}
	kv.data = make(map[string]string)
	kv.seen = make(map[string]int64)
	kv.processed = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
