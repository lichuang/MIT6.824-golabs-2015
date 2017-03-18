package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	// Your data here.
	lastSeq int // paxos process seq
	cfgnum  int // current largest config num
}

// op types
const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	// Your data here.
	Op      string
	Gid     int64
	Servers []string
	Shard   int
	UUID    int64
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, v := sm.px.Status(seq)
		//DPrintf("%d seq %d status: %d:%d\n", kv.me, seq, status, paxos.Decided)
		if status == paxos.Decided {
			log, _ := v.(Op)
			return log
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func getGidCounts(c *Config) (int64, int64) {
	min_id, min_num, max_id, max_num := int64(0), 999, int64(0), -1
	counts := map[int64]int{}
	for g := range c.Groups {
		counts[g] = 0
	}
	for _, g := range c.Shards {
		counts[g]++
	}
	for g := range counts {
		_, exists := c.Groups[g]
		if exists && min_num > counts[g] {
			min_id, min_num = g, counts[g]
		}
		if exists && max_num < counts[g] {
			max_id, max_num = g, counts[g]
		}
	}
	for _, g := range c.Shards {
		if g == 0 {
			max_id = 0
		}
	}
	return min_id, max_id
}

func getShardByGid(gid int64, c *Config) int {
	for s, g := range c.Shards {
		if g == gid {
			return s
		}
	}
	return -1
}

func (sm *ShardMaster) rebalance(gid int64, join bool) {
	c := &sm.configs[sm.cfgnum]
	for i := 0; ; i++ {
		min_id, max_id := getGidCounts(c)
		if !join {
			s := getShardByGid(gid, c)
			if s == -1 {
				break
			}
			c.Shards[s] = min_id
		} else {
			if i == NShards/len(c.Groups) {
				break
			}
			s := getShardByGid(max_id, c)
			c.Shards[s] = gid
		}
	}

}

func (sm *ShardMaster) nextConfig() *Config {
	old := &sm.configs[sm.cfgnum]
	var new Config
	new.Num = old.Num + 1
	new.Groups = map[int64][]string{}
	new.Shards = [NShards]int64{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, v := range old.Shards {
		new.Shards[i] = v
	}
	sm.cfgnum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.cfgnum]
}

func (sm *ShardMaster) applyJoin(gid int64, servers []string) {
	config := sm.nextConfig()
	_, exists := config.Groups[gid]
	if !exists {
		config.Groups[gid] = servers
		sm.rebalance(gid, true)
	}
}

func (sm *ShardMaster) applyLeave(gid int64) {
	config := sm.nextConfig()
	_, exists := config.Groups[gid]
	if exists {
		delete(config.Groups, gid)
		sm.rebalance(gid, false)
	}
}

func (sm *ShardMaster) applyMove(gid int64, shard int) {
	config := sm.nextConfig()
	config.Shards[shard] = gid
}

func (sm *ShardMaster) apply(op Op) {
	sm.lastSeq++
	switch op.Op {
	case Join:
		sm.applyJoin(op.Gid, op.Servers)
	case Leave:
		sm.applyLeave(op.Gid)
	case Move:
		sm.applyMove(op.Gid, op.Shard)
	}

	sm.px.Done(sm.lastSeq)
}

func (sm *ShardMaster) processOperation(op Op) {
	ok := false
	var log Op
	for !ok {
		seq := sm.lastSeq + 1

		status, val := sm.px.Status(seq)

		if status == paxos.Decided {
			log = val.(Op)
		} else {
			sm.px.Start(seq, op)
			log = sm.wait(seq)
		}

		ok = op.UUID == log.UUID
		sm.apply(log)
		if ok {
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Op:      Join,
		Gid:     args.GID,
		Servers: args.Servers,
		UUID:    nrand(),
	}

	sm.processOperation(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Op:   Leave,
		Gid:  args.GID,
		UUID: nrand(),
	}

	sm.processOperation(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Op:    Move,
		Gid:   args.GID,
		Shard: args.Shard,
		UUID:  nrand(),
	}

	sm.processOperation(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Op:   Query,
		UUID: nrand(),
	}

	sm.processOperation(op)

	if args.Num == -1 {
		reply.Config = sm.configs[sm.cfgnum]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	// Your initialization code here.
	sm.lastSeq = 1
	sm.cfgnum = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
