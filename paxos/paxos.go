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
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
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

type PaxosInstance struct {
	mu         sync.Mutex
	n_p        int         // max prepare
	n_a        int         // max accept
	v_a        interface{} // max accept val
	decided    bool
	decidedVal interface{}
	proposing  bool // is_actively_proposing?
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances      map[int]*PaxosInstance
	doneSeqs       []int // max Done() for each peer
	maxSeq         int
	maxProposalNum int
}

type PrepareArgs struct {
	Seq  int
	N    int
	Me   int
	Done int
}

type PrepareReply struct {
	OK   bool
	N_A  int
	V_A  interface{}
	N_P  int
	Done int
}

type AcceptArgs struct {
	Seq  int
	N    int
	V    interface{}
	Me   int
	Done int
}

type AcceptReply struct {
	OK   bool
	N    int
	Done int
}

type DecidedArgs struct {
	Seq  int
	V    interface{}
	Me   int
	Done int
}

type DecidedReply struct {
	Done int
}

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

func (px *Paxos) getInstance(seq int) *PaxosInstance {
	px.mu.Lock()
	defer px.mu.Unlock()

	if inst, exists := px.instances[seq]; exists {
		return inst
	}

	inst := &PaxosInstance{
		n_p: -1,
		n_a: -1,
	}
	px.instances[seq] = inst
	if seq > px.maxSeq {
		px.maxSeq = seq
	}
	return inst
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	if px.isdead() {
		return nil
	}

	px.mu.Lock()
	if args.Done > px.doneSeqs[args.Me] {
		px.doneSeqs[args.Me] = args.Done
	}
	reply.Done = px.doneSeqs[px.me]
	px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if args.N > inst.n_p {
		inst.n_p = args.N
		reply.N_A = inst.n_a
		reply.V_A = inst.v_a
		reply.OK = true
	} else {
		reply.OK = false
		reply.N_P = inst.n_p
	}

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	if px.isdead() {
		return nil
	}

	px.mu.Lock()
	if args.Done > px.doneSeqs[args.Me] {
		px.doneSeqs[args.Me] = args.Done
	}
	reply.Done = px.doneSeqs[px.me]
	px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if args.N >= inst.n_p {
		inst.n_p = args.N
		inst.n_a = args.N
		inst.v_a = args.V
		reply.OK = true
	} else {
		reply.OK = false
		reply.N = inst.n_p
	}

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	if px.isdead() {
		return nil
	}

	px.mu.Lock()
	if args.Done > px.doneSeqs[args.Me] {
		px.doneSeqs[args.Me] = args.Done
	}
	reply.Done = px.doneSeqs[px.me]
	px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	inst.mu.Lock()
	defer inst.mu.Unlock()

	inst.decided = true
	inst.decidedVal = args.V

	return nil
}

func (px *Paxos) propose(seq int, v interface{}) {
	for !px.isdead() {
		if fate, _ := px.Status(seq); fate == Decided {
			return
		}

		proposalNum := px.generateProposalNumber()

		// prepare phase
		prepareCount := 0
		maxAcceptNum := -1
		var maxAcceptVal interface{} = nil

		for i := 0; i < len(px.peers); i++ {
			args := &PrepareArgs{
				Seq:  seq,
				N:    proposalNum,
				Me:   px.me,
				Done: px.getDoneValue(),
			}
			var reply PrepareReply

			if ok := px.sendPrepare(i, args, &reply); ok {
				if reply.OK {
					prepareCount++
					if reply.N_A > maxAcceptNum {
						maxAcceptNum = reply.N_A
						maxAcceptVal = reply.V_A
					}
				} else {
					px.updateMaxProposalNumber(reply.N_P)
				}
				px.updatePeerDone(i, reply.Done)
			}

			if prepareCount > len(px.peers)/2 {
				break
			}
		}

		// accept phase
		if prepareCount > len(px.peers)/2 {
			acceptVal := v
			if maxAcceptVal != nil {
				acceptVal = maxAcceptVal
			}

			acceptCount := 0
			for i := 0; i < len(px.peers); i++ {
				args := &AcceptArgs{
					Seq:  seq,
					N:    proposalNum,
					V:    acceptVal,
					Me:   px.me,
					Done: px.getDoneValue(),
				}
				var reply AcceptReply

				if ok := px.sendAccept(i, args, &reply); ok {
					if reply.OK {
						acceptCount++
					} else {
						px.updateMaxProposalNumber(reply.N)
					}
					px.updatePeerDone(i, reply.Done)
				}

				if acceptCount > len(px.peers)/2 {
					break
				}
			}

			// decided phase
			if acceptCount > len(px.peers)/2 {
				px.broadcastDecision(seq, acceptVal)
				return
			}
		}

		// backoff before retry
		time.Sleep(px.getBackoffDuration())
	}
}

// helpers
func (px *Paxos) generateProposalNumber() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.maxProposalNum += len(px.peers)
	return px.maxProposalNum + px.me
}

func (px *Paxos) getDoneValue() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.doneSeqs[px.me]
}

func (px *Paxos) updatePeerDone(peer int, doneVal int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if doneVal > px.doneSeqs[peer] {
		px.doneSeqs[peer] = doneVal
	}
}

func (px *Paxos) getBackoffDuration() time.Duration {
	baseDelay := 10 * time.Millisecond
	maxDelay := 100 * time.Millisecond
	jitter := time.Duration(rand.Int63n(10)) * time.Millisecond
	return min(baseDelay+jitter, maxDelay)
}

func (px *Paxos) sendPrepare(peer int, args *PrepareArgs, reply *PrepareReply) bool {
	if peer == px.me {
		px.Prepare(args, reply)
		return true
	}
	return call(px.peers[peer], "Paxos.Prepare", args, reply)
}

func (px *Paxos) sendAccept(peer int, args *AcceptArgs, reply *AcceptReply) bool {
	if peer == px.me {
		px.Accept(args, reply)
		return true
	}
	return call(px.peers[peer], "Paxos.Accept", args, reply)
}

func (px *Paxos) broadcastDecision(seq int, v interface{}) {
	for i := 0; i < len(px.peers); i++ {
		args := &DecidedArgs{
			Seq:  seq,
			V:    v,
			Me:   px.me,
			Done: px.getDoneValue(),
		}
		var reply DecidedReply

		if i == px.me {
			px.Decided(args, &reply)
		} else {
			go func(peer int) {
				call(px.peers[peer], "Paxos.Decided", args, &reply)
			}(i)
		}
	}
}

func (px *Paxos) updateMaxProposalNumber(n int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if n > px.maxProposalNum {
		px.maxProposalNum = n
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	if seq < px.Min() {
		return
	}

	px.mu.Lock()
	if seq > px.maxSeq {
		px.maxSeq = seq
	}

	inst, exists := px.instances[seq]
	if !exists {
		inst = &PaxosInstance{
			n_p: -1,
			n_a: -1,
		}
		px.instances[seq] = inst
	}

	if !inst.proposing {
		inst.proposing = true
		px.mu.Unlock()
		go px.propose(seq, v)
	} else {
		px.mu.Unlock()
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// TODO: Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.doneSeqs[px.me] {
		px.doneSeqs[px.me] = seq
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	max := px.maxSeq
	for seq := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

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
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	min := math.MaxInt32
	for _, doneSeq := range px.doneSeqs {
		if doneSeq < min {
			min = doneSeq
		}
	}

	// clean up forgotten instances
	for seq := range px.instances {
		if seq <= min {
			delete(px.instances, seq)
		}
	}

	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// TODO: Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	inst := px.getInstance(seq)
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if inst.decided {
		return Decided, inst.decidedVal
	}

	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
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

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// TODO: Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.doneSeqs = make([]int, len(peers))
	for i := range px.doneSeqs {
		px.doneSeqs[i] = -1
	}
	px.maxSeq = -1
	px.maxProposalNum = 0

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
