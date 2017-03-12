package paxos

const (
	OK     = "OK"
	REJECT = "REJECT"
)

type PaxosArgs struct {
	PNum      string
	PValue    interface{}
	Me        int
	Done      int
	ProposeId int
}

type PaxosReply struct {
	Result string
	Pnum   string
	PValue interface{}
}

// for propose api
