package raft

import "fmt"

// RequestVoteArgs 由候选人负责调用用来征集选票
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 候选人的任期号
	Term int
	// 请求选票的候选人的 ID
	CandidateId int
	// 候选人的最后日志条目的索引值
	LastLogIndex int
	// 候选人的最后日志条目的任期号
	LastLogTerm int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v,CandidateId:%v,LastLogIndex:%v,LastLogTerm:%v}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 当前任期号，以便于候选人去更新自己的任期号
	Term int
	// 候选人赢得了此张选票时为真
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v,VoteGranted:%v}", reply.Term, reply.VoteGranted)
}

// AppendEntriesArgs 由leader调用，用于日志条目的复制，同时也被当做心跳使用
type AppendEntriesArgs struct {
	// 领导人的任期
	Term int
	// 领导人 ID 因此跟随者可以对客户端进行重定向（跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人)
	LeaderId int
	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIndex int
	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm int
	// 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []Entry
	// 领导人的已知已提交的最高的日志条目的索引
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PreLogIndex:%v,PreLogTerm:%v,LeaderCommit:%v,Entries:%v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

type AppendEntriesReply struct {
	// 当前任期，对于领导人而言 它会更新自己的任期
	Term int
	// 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Success bool
	//	快速恢复
	XTerm  int
	XIndex int
	XLen   int
}

func (response AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,XTerm:%v,XIndex:%v,XLen:%v}", response.Term, response.Success, response.XTerm, response.XIndex, response.XLen)
}
