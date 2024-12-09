package shardkv

import (
	"cs134-24f-kv/paxos"
	"cs134-24f-kv/shardmaster"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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

type ShardState int

const (
	Serving ShardState = iota
	Pulling
	Missing
)

type Op struct {
	// Your definitions here.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64
	Config    shardmaster.Config
	Shard     int
	Data      map[string]string
	ClientSeq map[int64]int64
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

	// TODO: Your definitions here.
	config       shardmaster.Config
	kvstore      map[int]map[string]string
	clientSeq    map[int64]int64
	lastApplied  int
	shardStates  map[int]ShardState
	pullingShard sync.WaitGroup
}

func (kv *ShardKV) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// TODO: Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)

	if kv.config.Shards[shard] != kv.gid || kv.shardStates[shard] != Serving {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return nil
	}

	if seq, ok := kv.clientSeq[args.ClientId]; ok && args.SeqNum <= seq {
		if args.SeqNum == seq {
			if value, exists := kv.kvstore[shard][args.Key]; exists {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		}
		kv.mu.Unlock()
		return nil
	}

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	seq := kv.lastApplied + 1
	kv.px.Start(seq, op)
	kv.mu.Unlock()

	decided := kv.wait(seq)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.apply(seq)

	if kv.config.Shards[shard] != kv.gid || kv.shardStates[shard] != Serving {
		reply.Err = ErrWrongGroup
		return nil
	}

	if decided.Type == op.Type && decided.ClientId == op.ClientId && decided.SeqNum == op.SeqNum {
		if value, exists := kv.kvstore[shard][args.Key]; exists {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		kv.clientSeq[args.ClientId] = args.SeqNum
		return nil
	}

	reply.Err = ErrWrongGroup
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// TODO: Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	DPrintf("GID %d Server %d: %s request for key %s (shard %d, config %d)\n",
		kv.gid, kv.me, args.Op, args.Key, shard, kv.config.Num)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	seq := kv.lastApplied + 1
	kv.px.Start(seq, op)
	decided := kv.wait(seq)
	kv.apply(seq)

	if decided.Type == op.Type && decided.ClientId == op.ClientId && decided.SeqNum == op.SeqNum {
		reply.Err = OK
		return nil
	}

	reply.Err = ErrWrongGroup
	return nil
}

func (kv *ShardKV) apply(seq int) {
	for i := kv.lastApplied + 1; i <= seq; i++ {
		decided, value := kv.px.Status(i)
		if decided == paxos.Decided {
			op := value.(Op)
			shard := key2shard(op.Key)

			switch op.Type {
			case "Transfer":
				if kv.kvstore[op.Shard] == nil {
					kv.kvstore[op.Shard] = make(map[string]string)
				}
				for k, v := range op.Data {
					kv.kvstore[op.Shard][k] = v
				}
				for cid, seq := range op.ClientSeq {
					if oldSeq, exists := kv.clientSeq[cid]; !exists || seq > oldSeq {
						kv.clientSeq[cid] = seq
					}
				}
				kv.shardStates[op.Shard] = Serving

			case "Reconfigure":
				if op.Config.Num == kv.config.Num+1 {
					kv.config = op.Config
				}

			default:
				if kv.config.Shards[shard] == kv.gid && kv.shardStates[shard] == Serving {
					if op.SeqNum > kv.clientSeq[op.ClientId] {
						if kv.kvstore[shard] == nil {
							kv.kvstore[shard] = make(map[string]string)
						}
						switch op.Type {
						case "Put":
							kv.kvstore[shard][op.Key] = op.Value
						case "Append":
							oldVal := kv.kvstore[shard][op.Key]
							kv.kvstore[shard][op.Key] = oldVal + op.Value
						}
						kv.clientSeq[op.ClientId] = op.SeqNum
					}
				}
			}
		}
	}
	kv.lastApplied = seq
	kv.px.Done(seq)
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num != args.ConfigNum && kv.config.Num != args.ConfigNum+1 {
		DPrintf("GID %d Server %d: Wrong config for transfer. Have %d, want %d\n",
			kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		reply.Err = ErrWrongGroup
		return nil
	}

	DPrintf("GID %d Server %d: Transferring shard %d with data\n",
		kv.gid, kv.me, args.Shard)

	reply.Err = OK
	reply.KV = make(map[string]string)
	reply.ClientSeq = make(map[int64]int64)

	if data := kv.kvstore[args.Shard]; data != nil {
		for k, v := range data {
			reply.KV[k] = v
		}
	}

	for cid, seq := range kv.clientSeq {
		reply.ClientSeq[cid] = seq
	}

	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)
	if newConfig.Num <= kv.config.Num {
		return
	}

	nextConfig := kv.sm.Query(kv.config.Num + 1)
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if nextConfig.Shards[shard] == kv.gid && kv.config.Shards[shard] != kv.gid {
			kv.shardStates[shard] = Pulling
		}
	}
	allReady := true
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.shardStates[shard] == Pulling {
			oldGid := kv.config.Shards[shard]
			if oldGid != 0 {
				if servers, ok := kv.config.Groups[oldGid]; ok {
					transferred := false
					for _, srv := range servers {
						args := &TransferArgs{
							Shard:     shard,
							ConfigNum: kv.config.Num,
						}
						var reply TransferReply
						ok := call(srv, "ShardKV.Transfer", args, &reply)
						if ok && reply.Err == OK {
							op := Op{
								Type:      "Transfer",
								Shard:     shard,
								Data:      reply.KV,
								ClientSeq: reply.ClientSeq,
							}
							seq := kv.lastApplied + 1
							kv.px.Start(seq, op)
							kv.wait(seq)
							kv.apply(seq)
							kv.shardStates[shard] = Serving
							transferred = true
							break
						}
					}
					if !transferred {
						allReady = false
					}
				}
			} else {
				kv.shardStates[shard] = Serving
				if kv.kvstore[shard] == nil {
					kv.kvstore[shard] = make(map[string]string)
				}
			}
		}
	}

	if allReady {
		op := Op{
			Type:   "Reconfigure",
			Config: nextConfig,
		}
		seq := kv.lastApplied + 1
		kv.px.Start(seq, op)
		kv.wait(seq)
		kv.apply(seq)

		for shard := 0; shard < shardmaster.NShards; shard++ {
			if nextConfig.Shards[shard] == kv.gid {
				if kv.shardStates[shard] != Serving {
					kv.shardStates[shard] = Serving
				}
			} else {
				kv.shardStates[shard] = Missing
			}
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

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// TODO: Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{Num: 0, Shards: [10]int64{}, Groups: map[int64][]string{}}
	kv.kvstore = make(map[int]map[string]string)
	kv.clientSeq = make(map[int64]int64)
	kv.shardStates = make(map[int]ShardState)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardStates[i] = Missing
	}

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
