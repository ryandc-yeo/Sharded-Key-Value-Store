package shardmaster

import (
	"cs134-24f-kv/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	done    int
}

type Op struct {
	// TODO: Your data here.
	Type    string
	GID     int64    // Join, Leave, Move
	Shard   int      // Move
	Servers []string // Join
	Num     int      // Query
}

func (sm *ShardMaster) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, value := sm.px.Status(seq)
		if status == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) propose(op Op) Config {
	for {
		seq := sm.done + 1
		status, value := sm.px.Status(seq)

		if status != paxos.Decided {
			sm.px.Start(seq, op)
			op1 := sm.wait(seq)
			if op1.Type == op.Type && op1.GID == op.GID {
				cfg := sm.applyOp(op1)
				sm.done = seq
				sm.px.Done(seq)
				return cfg
			}
		} else {
			sm.applyOp(value.(Op))
			sm.done = seq
			sm.px.Done(seq)
		}
	}
}

func (sm *ShardMaster) applyOp(op Op) Config {
	switch op.Type {
	case JOIN:
		return sm.applyJoin(op.GID, op.Servers)
	case LEAVE:
		return sm.applyLeave(op.GID)
	case MOVE:
		return sm.applyMove(op.Shard, op.GID)
	case QUERY:
		if op.Num == -1 || op.Num >= len(sm.configs) {
			return sm.configs[len(sm.configs)-1]
		}
		return sm.configs[op.Num]
	}
	return Config{}
}

func (sm *ShardMaster) applyJoin(gid int64, servers []string) Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}

	newConfig.Groups[gid] = append([]string{}, servers...)
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) applyLeave(gid int64) Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	for g, servers := range lastConfig.Groups {
		if g != gid {
			newConfig.Groups[g] = append([]string{}, servers...)
		}
	}
	for i, g := range newConfig.Shards {
		if g == gid {
			newConfig.Shards[i] = 0
		}
	}
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) applyMove(shard int, gid int64) Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:    len(sm.configs),
		Groups: make(map[int64][]string),
		Shards: lastConfig.Shards,
	}

	for g, servers := range lastConfig.Groups {
		newConfig.Groups[g] = append([]string{}, servers...)
	}

	newConfig.Shards[shard] = gid
	sm.configs = append(sm.configs, newConfig)
	return newConfig
}

func (sm *ShardMaster) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	counts := make(map[int64]int)
	for _, gid := range config.Shards {
		if gid != 0 {
			counts[gid]++
		}
	}
	var gids []int64
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})

	target := NShards / len(config.Groups)
	for shard := range config.Shards {
		if config.Shards[shard] == 0 {
			var minGid int64
			minShards := NShards + 1

			for _, gid := range gids {
				if counts[gid] < minShards {
					minGid = gid
					minShards = counts[gid]
				}
			}

			config.Shards[shard] = minGid
			counts[minGid]++
		}
	}

	for {
		var maxGid, minGid int64
		maxShards := -1
		minShards := NShards + 1
		for _, gid := range gids {
			count := counts[gid]
			if count > maxShards {
				maxGid = gid
				maxShards = count
			}
			if count < minShards {
				minGid = gid
				minShards = count
			}
		}

		if maxShards <= target+1 && minShards >= target {
			break
		}
		for shard := range config.Shards {
			if config.Shards[shard] == maxGid {
				config.Shards[shard] = minGid
				counts[maxGid]--
				counts[minGid]++
				break
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type:    JOIN,
		GID:     args.GID,
		Servers: args.Servers,
	}
	sm.propose(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: LEAVE,
		GID:  args.GID,
	}
	sm.propose(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type:  MOVE,
		GID:   args.GID,
		Shard: args.Shard,
	}
	sm.propose(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// TODO: Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: QUERY,
		Num:  args.Num,
	}

	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.propose(op)
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

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

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
