package kvpaxos

import (
	"cs134-24f-kv/paxos"
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

type Op struct {
	// TODO: Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // put/append/get
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// TODO: Your definitions here.
	kvstore     map[string]string
	clientSeqs  map[int64]int
	lastExecSeq int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// TODO: Your code here.
	if kv.isdead() {
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastSeq, exists := kv.clientSeqs[args.ClientId]; exists && args.RequestId <= lastSeq {
		reply.Value = kv.kvstore[args.Key]
		reply.Err = OK
		return nil
	}

	seq := kv.px.Max() + 1
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	for !kv.isdead() {
		kv.px.Start(seq, op)
		decidedOp := kv.wait(seq)
		kv.sync(seq)

		if decidedOp.ClientId == op.ClientId && decidedOp.RequestId == op.RequestId {
			reply.Value = kv.kvstore[args.Key]
			reply.Err = OK
			return nil
		}
		seq++
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// TODO: Your code here.
	if kv.isdead() {
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastSeq, exists := kv.clientSeqs[args.ClientId]; exists && args.RequestId <= lastSeq {
		reply.Err = OK
		return nil
	}

	seq := kv.px.Max() + 1
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	for !kv.isdead() {
		kv.px.Start(seq, op)
		decidedOp := kv.wait(seq)
		kv.sync(seq)

		if decidedOp.ClientId == op.ClientId && decidedOp.RequestId == op.RequestId {
			reply.Err = OK
			return nil
		}
		seq++
	}
	return nil
}

// helpers

func (kv *KVPaxos) wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if kv.isdead() {
			return Op{}
		}

		status, decidedValue := kv.px.Status(seq)
		if status == paxos.Decided {
			return decidedValue.(Op)
		}

		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
}

func (kv *KVPaxos) updateState(op Op) {
	if lastSeq, exists := kv.clientSeqs[op.ClientId]; !exists || op.RequestId > lastSeq {
		kv.clientSeqs[op.ClientId] = op.RequestId

		switch op.Type {
		case "Put":
			kv.kvstore[op.Key] = op.Value
		case "Append":
			kv.kvstore[op.Key] = kv.kvstore[op.Key] + op.Value
		}
	}
}

func (kv *KVPaxos) sync(seq int) {
	for i := kv.lastExecSeq + 1; i <= seq; i++ {
		decidedOp := kv.wait(i)
		kv.updateState(decidedOp)
		kv.lastExecSeq = i
		kv.px.Done(i)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// TODO: Your initialization code here.
	kv.kvstore = make(map[string]string)
	kv.clientSeqs = make(map[int64]int)
	kv.lastExecSeq = -1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
