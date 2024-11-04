package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// TODO: You'll have to add definitions here.
	Op       string
	ReqID    int64
	ClientID int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// TODO: You'll have to add definitions here.
	ReqID    int64
	ClientID int64
}

type GetReply struct {
	Err   Err
	Value string
}

// TODO: Your RPC definitions here.
type ForwardPutAppendArgs struct {
	Key      string
	Value    string
	Op       string
	ReqID    int64
	ClientID int64
}

type ForwardPutAppendReply struct {
	Err Err
}

type ForwardGetArgs struct {
	Key      string
	ReqID    int64
	ClientID int64
}

type ForwardGetReply struct {
	Err Err
}

type TransferDBArgs struct {
}

type TransferDBReply struct {
	Err          Err
	DB           map[string]string
	SeenRequests map[int64]string
	SeenClients  map[int64]int64
}

// type TransferStateArgs struct {
// 	KeyValue  map[string]string
// 	Completed map[int64]int64
// }

// type TransferStateReply struct {
// 	Err Err
// }
