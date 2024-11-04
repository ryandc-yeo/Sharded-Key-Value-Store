package pbservice

import (
	"cs134-24f-kv/viewservice"
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

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// TODO: Your declarations here.
	// done   sync.WaitGroup
	// finish chan interface{}
	view         viewservice.View
	db           map[string]string
	seenRequests map[int64]string
	seenClients  map[int64]int64
	transferring bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// TODO: Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	// check for duplicate req
	if val, ok := pb.seenRequests[args.ReqID]; ok {
		reply.Value = val
		reply.Err = OK
		return nil
	}

	value, exists := pb.db[args.Key]
	if !exists {
		value = ""
	}

	if pb.view.Backup != "" {
		fwdArgs := &ForwardGetArgs{
			Key:      args.Key,
			ReqID:    args.ReqID,
			ClientID: args.ClientID,
		}
		var fwdReply ForwardGetReply

		ok := call(pb.view.Backup, "PBServer.ForwardGet", fwdArgs, &fwdReply)
		if !ok || fwdReply.Err != OK {
			if pb.isunreliable() {
			}
		}
	}

	// // forward the get req if we have backup
	// if pb.view.Backup != "" {
	// 	backupArgs := *args
	// 	var backupReply GetReply
	// 	ok := call(pb.view.Backup, "PBServer.Get", &backupArgs, &backupReply)
	// 	if !ok {
	// 		reply.Err = ErrWrongServer
	// 		return nil
	// 	}
	// }
	pb.seenRequests[args.ReqID] = value
	pb.seenClients[args.ClientID] = args.ReqID

	reply.Value = value
	reply.Err = OK
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// TODO: Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if _, isDuplicate := pb.seenRequests[args.ReqID]; isDuplicate {
		reply.Err = OK
		return nil
	}

	// if primary, forward to backup if exists
	if pb.view.Backup != "" {
		fwdArgs := &ForwardPutAppendArgs{
			Key:      args.Key,
			Value:    args.Value,
			Op:       args.Op,
			ReqID:    args.ReqID,
			ClientID: args.ClientID,
		}
		var fwdReply ForwardPutAppendReply

		ok := call(pb.view.Backup, "PBServer.ForwardPutAppend", fwdArgs, &fwdReply)
		if !ok || fwdReply.Err != OK {
			if !pb.isunreliable() {
				reply.Err = ErrWrongServer
				return nil
			}
		}
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else if args.Op == "Append" {
		pb.db[args.Key] += args.Value
	}

	pb.seenRequests[args.ReqID] = ""
	pb.seenClients[args.ClientID] = args.ReqID
	reply.Err = OK
	return nil
}

func (pb *PBServer) ForwardGet(args *ForwardGetArgs, reply *ForwardGetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.seenRequests[args.ReqID] = ""
	pb.seenClients[args.ClientID] = args.ReqID

	reply.Err = OK
	return nil
}

func (pb *PBServer) ForwardPutAppend(args *ForwardPutAppendArgs, reply *ForwardPutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else if args.Op == "Append" {
		pb.db[args.Key] += args.Value
	}

	pb.seenRequests[args.ReqID] = ""
	pb.seenClients[args.ClientID] = args.ReqID

	reply.Err = OK
	return nil
}

func (pb *PBServer) TransferDB(args *TransferDBArgs, reply *TransferDBReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	reply.DB = make(map[string]string)
	reply.SeenRequests = make(map[int64]string)
	reply.SeenClients = make(map[int64]int64)

	for k, v := range pb.db {
		reply.DB[k] = v
	}
	for k, v := range pb.seenRequests {
		reply.SeenRequests[k] = v
	}
	for k, v := range pb.seenClients {
		reply.SeenClients[k] = v
	}

	reply.Err = OK
	return nil
}

// func (pb *PBServer) requestDBTransfer(primary string) {
// 	args := &TransferDBArgs{}
// 	var reply TransferDBReply

// 	ok := call(primary, "PBServer.TransferDB", args, &reply)
// 	if ok && reply.Err == OK {
// 		pb.db = make(map[string]string)
// 		for k, v := range reply.DB {
// 			pb.db[k] = v
// 		}
// 		pb.seenRequests = make(map[int64]string)
// 		for k, v := range reply.SeenRequests {
// 			pb.seenRequests[k] = v
// 		}
// 		pb.seenClients = make(map[int64]int64)
// 		for k, v := range reply.SeenClients {
// 			pb.seenClients[k] = v
// 		}
// 	}
// }

// func (pb *PBServer) TransferState(args *TransferStateArgs, reply *TransferStateReply) error {
// 	pb.mu.Lock()
// 	defer pb.mu.Unlock()

// 	if !pb.backup {
// 		reply.Err = ErrWrongServer
// 		return nil
// 	}

// 	pb.keyValue = make(map[string]string)
// 	pb.completed = make(map[int64]int64)

// 	for k, v := range args.KeyValue {
// 		pb.keyValue[k] = v
// 	}
// 	for k, v := range args.Completed {
// 		pb.completed[k] = v
// 	}

// 	reply.Err = OK
// 	return nil
// }

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		if view.Viewnum != pb.view.Viewnum ||
			(view.Backup == pb.me && pb.view.Backup != pb.me) {

			if view.Backup == pb.me && pb.view.Backup != pb.me && view.Primary != "" {
				args := &TransferDBArgs{}
				var reply TransferDBReply

				ok := call(view.Primary, "PBServer.TransferDB", args, &reply)
				if ok && reply.Err == OK {
					newDB := make(map[string]string)
					newReqs := make(map[int64]string)
					newClients := make(map[int64]int64)

					for k, v := range reply.DB {
						newDB[k] = v
					}
					for k, v := range reply.SeenRequests {
						newReqs[k] = v
					}
					for k, v := range reply.SeenClients {
						newClients[k] = v
					}

					pb.db = newDB
					pb.seenRequests = newReqs
					pb.seenClients = newClients
				}
			}

			pb.view = view
		}
	}
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
	// TODO: Your pb.* initializations here.

	// pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.view = viewservice.View{}
	pb.db = make(map[string]string)
	pb.seenRequests = make(map[int64]string)
	pb.seenClients = make(map[int64]int64)
	// pb.dead = 0
	// pb.unreliable = 0
	// pb.transferring = false

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
