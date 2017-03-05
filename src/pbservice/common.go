package pbservice

import (
	"viewservice"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
	Put    = "Put"
	Append = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	Seq int64
	Op  string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type SyncArgs struct {
	Data map[string]string
	Seen map[int64]bool
	View viewservice.View
}

type SyncReply struct {
}

type SyncUpdateArgs struct {
	Key   string
	Value string
	Seq   int64
}

type SyncUpdateReply struct {
}
