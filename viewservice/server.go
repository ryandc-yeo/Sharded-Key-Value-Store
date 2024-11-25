package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// TODO: Your declarations here.
	currentView View
	lastPing    map[string]time.Time
	ackdView    uint
	hasStarted  bool
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// TODO: Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.lastPing[args.Me] = time.Now()

	// init first view when first server starts up
	if !vs.hasStarted {
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum = 1
		vs.hasStarted = true
		vs.ackdView = 0
		reply.View = vs.currentView
		return nil
	}

	// handle primary ack
	if args.Me == vs.currentView.Primary {
		if args.Viewnum == vs.currentView.Viewnum {
			vs.ackdView = args.Viewnum
		} else if args.Viewnum == 0 {
			if vs.canChangeView() {
				vs.promotePrimaryBackup()
			}
		}
	}

	// handle backup ack
	if args.Me == vs.currentView.Backup && args.Viewnum == 0 {
		if vs.canChangeView() {
			vs.currentView.Backup = ""
			vs.currentView.Viewnum++
		}
	}

	// add new backup if needed
	if vs.currentView.Backup == "" && args.Me != vs.currentView.Primary && vs.canChangeView() {
		vs.currentView.Backup = args.Me
		vs.currentView.Viewnum++
	}

	// if args.Viewnum == 0 {
	// 	if args.Me == vs.currentView.Primary {
	// 		if vs.canChangeView() {
	// 			vs.promotePrimaryBackup()
	// 		}
	// 	}
	// 	if args.Me == vs.currentView.Backup {
	// 		if vs.canChangeView() {
	// 			vs.currentView.Backup = ""
	// 			vs.currentView.Viewnum++
	// 		}
	// 	}
	// }

	// // Handle first server startup
	// if !vs.hasStarted {
	// 	vs.currentView.Primary = args.Me
	// 	vs.currentView.Viewnum = 1
	// 	vs.hasStarted = true
	// }

	// if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum {
	// 	vs.ackdView = args.Viewnum
	// }

	// if vs.currentView.Backup == "" && args.Me != vs.currentView.Primary && vs.canChangeView() {
	// 	vs.currentView.Backup = args.Me
	// 	vs.currentView.Viewnum++
	// }

	reply.View = vs.currentView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// TODO: Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.currentView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {

	// TODO: Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if !vs.hasStarted {
		return
	}

	now := time.Now()

	// is primary dead
	if vs.currentView.Primary != "" {
		lastPing, ok := vs.lastPing[vs.currentView.Primary]
		if !ok || now.Sub(lastPing) >= DeadPings*PingInterval {
			if vs.canChangeView() {
				vs.promotePrimaryBackup()
			}
		}
	}

	// is backup dead
	if vs.currentView.Backup != "" {
		lastPing, ok := vs.lastPing[vs.currentView.Backup]
		if !ok || now.Sub(lastPing) >= DeadPings*PingInterval {
			if vs.canChangeView() {
				vs.currentView.Backup = ""
				vs.currentView.Viewnum++
			}
		}
	}

	// if we need to find a new backup
	if vs.currentView.Backup == "" && vs.canChangeView() {
		for server, lastPing := range vs.lastPing {
			if server != vs.currentView.Primary && now.Sub(lastPing) < DeadPings*PingInterval {
				vs.currentView.Backup = server
				vs.currentView.Viewnum++
				break
			}
		}
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

// has this server been asked to shut down?
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
	// TODO: Your vs.* initializations here.
	vs.currentView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.lastPing = make(map[string]time.Time)
	vs.ackdView = 0
	vs.hasStarted = false

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

// HELPER FUNCTIONS
// check if we can change to a new view
func (vs *ViewServer) canChangeView() bool {
	return vs.ackdView == vs.currentView.Viewnum
}

// promote backup to primary
func (vs *ViewServer) promotePrimaryBackup() {
	if vs.currentView.Backup != "" {
		vs.currentView.Primary = vs.currentView.Backup
		vs.currentView.Backup = ""
		vs.currentView.Viewnum++
	} else {
		vs.currentView.Primary = ""
		vs.currentView.Viewnum++
	}
}
