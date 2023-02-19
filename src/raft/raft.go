package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	// 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	currentTerm int
	// 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	// 即使节点重启，Raft 算法也能保证每个任期最多只有一个 leader。
	votedFor int
	// 已经 committed 的日志，保证状态机可恢复。
	logs []Entry

	// 所有服务器上的易失性状态
	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	//leader 节点重启后可以通过 appendEntries rpc 逐渐得到不同节点的 matchIndex，从而确认 commitIndex
	//follower 只需等待 leader 传递过来的 commitIndex 即可
	commitIndex int
	// 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int

	// 领导人（服务器）上的易失性状态 每次选举后，leader 的此两个数组都应该立刻重新初始化并开始探测
	// 为每一个 follower 保存的，应该发送的下一份 entry index（初始值为领导人最后的日志条目的索引 +1  , last index + 1。）
	nextIndex []int
	// 已确认的，已经同步到每一个 follower 的 entry index（初始值为0，根据复制状态不断递增，）
	matchIndex []int

	state   State
	applyCh chan ApplyMsg
	// 用于在提交新条目后唤醒 applier goroutine
	applyCond *sync.Cond
	// 用于通知复制器 goroutine 批量复制条目
	replicatorCond []*sync.Cond
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

func (rf *Raft) changeState(state State) {
	if rf.state == state {
		return
	}
	DPrintf("[Node %d] changes state from %v to %v in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomElectionTimeOut())
	case Candidate:
	case Leader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(FixedHeartBeatTimeout())
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?

		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	lastLog := rf.getLastLog()
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}

// 候选者希望发起election
func (rf *Raft) startElection() {
	args := rf.genRequestVoteArgs()
	DPrintf("[Node %v] starts election with RequestVoteArgs: %v", rf.me, args)
	//先给自己投一票
	rf.votedFor = rf.me
	rf.persist()
	grantedVotes := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		//异步向每个peer发起投票
		go func(peer int) {
			reply := new(RequestVoteReply)
			//调用sendRequestVote RPC 给每个peer
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[Node %v] receives RequestVoteReply %v from [Node %v] after sending RequestVoteArgs %v in term %v", rf.me, args, peer, reply, rf.currentTerm)
				//	一旦这个节点不在是candidate或者term增加了，后续传过来的投票就过期，丢弃
				// rf我们无法控制，只能通过加锁的方式控制在当前函数运行时不变，但是在此之前可能已经发生了变化
				// args代表rf(先前)任期的情况，reply和args是相同的term
				if rf.state == Candidate && rf.currentTerm == args.Term {
					if reply.VoteGranted {
						// 注意这里是由于在外面是异步的，一旦超过半数就立即作为leader
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("[Node %v] receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.changeState(Leader)
							// 当选leader后立即发送心跳信号
							rf.broadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("[Node %v] finds a new leader [Node %v] with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.changeState(Follower)
						// rf改变状态，开启下一轮
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

// RequestVote - sendRequestVote RPC实际对应的函数
// example RequestVote RPC handler.
// 注意： 只有在 grant 投票时才重置选举超时时间，这样有助于网络不稳定条件下选主的 liveness 问题
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("[Node %v]'s state is {state %v,term %v} before processing requestVoteRequest %v and reply requestVoteResponse %v",
		rf.me, rf.state, rf.currentTerm, args, reply)
	//	过期请求
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 一个Term只能给一个peer投票
	//	当前任期已经投过票，不能在投票了
	// 注意：这是args.Term == rf.currentTerm前提下
	// rf.votedFor == -1 表示拥有投票权力
	// rf.voteFor != args.CandidateID 表示给被别人投过票
	// 那么rf.voteFor 需不需要额外处理？？？
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 收到一个更高任期的RequestVoteArgs RPC，变为Follower, 采用这个新任期，清空votedFor, 从而重新获得投票权
	// 注意: 如果你当前不是follower, 需要重置选举计时器; 如果是, 不要重置选举计时器!
	// 因为它可能被其他候选者无限打断, 候选者总是在任期上占优！ ？？？
	if args.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// Raft 在投票阶段就确保选举出的 leader 一定包含了整个集群中目前已 committed 的所有日志。
	// 判断日志新旧的方式：获取请求的 entry 后，比对自己日志中的最后一个 entry。
	//首先比对 term，如果自己的 term 更大，则拒绝请求。
	//如果 term 一样，则比对 index，如果自己的 index 更大（说明自己的日志更长），则拒绝请求
	if args.LastLogTerm < rf.getLastLog().Term || (args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex < rf.getLastLog().Index) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 注意：这是args.Term == rf.currentTerm前提下，且没有投过票
	rf.votedFor = args.CandidateId
	// 为什么要还原？ -》投完票开启下一轮
	rf.electionTimer.Reset(RandomElectionTimeOut())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

func (rf *Raft) reInitLeaderState() {
	//	log处理
	//lastLog := rf.getLastLog()
	//for i:=0;i<len(rf.peers);i++{
	//	rf.nex
	//}
	rf.heartbeatTimer.Reset(FixedHeartBeatTimeout())
	rf.electionTimer.Stop()
}

func (rf *Raft) broadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			go rf.doReplicate(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

// 寻找共识点时，leader 还是通过 AppendEntriesRPC 和 follower 进行一致性检查，
// 方法是发送再上一块的 entry， 如果 follower 依然拒绝，则 leader 再尝试发送更前面的一块，直到找到双方的共识点。
func (rf *Raft) doReplicate(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	// 注意：对于一个新当选的leader，rf.nextIndex[peer]初始化为rf.getLastLog().Index + 1
	// 此时prevLogIndex就是leader最后一条log的index
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex >= rf.getFirstLog().Index {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, reply)
			rf.mu.Unlock()
		}
	} else { // need snapshot
		//args := rf.genInstallSnapshotArgs()
		//rf.mu.RUnlock()
		//reply := new(InstallSnapshotReply)
		//if rf.sendInstallSnapshot(peer, args, reply) {
		//	rf.mu.Lock()
		//	rf.handleInstallSnapshotReply(peer, args, reply)
		//	rf.mu.Unlock()
		//}
	}
}
func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
	// 深拷贝，snapshot时可能会把entry删掉，导致发送的RPC里entries引用不到
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:])
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	defer DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"before processing AppendEntriesArgs %v and reply AppendEntriesReply %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	// 返回假 如果领导人的任期小于接收者的当前任期 这里的接收者是指跟随者或者候选人
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 更新自己的任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// 更新自己状态为Follower
	rf.changeState(Follower)
	//	 leader的PrevLogIndex比自己的第一条log的index还小？？
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term = 0
		reply.Success = false
		DPrintf("[Node %v] receives unexpected AppendEntriesRequest %v from [Node %v] because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}
	// 在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假
	lastLogIndex := rf.getLastLog().Index
	firstLogIndex := rf.getFirstLog().Index
	//prevLogIndex > rf.getLastLog().Index : 当前follower存在log缺失
	//rf.logs[preLogIndex - firstLogIndex].Term != Term Term不匹配，存在日志冲突
	if args.PrevLogIndex > lastLogIndex ||
		rf.logs[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 当前Follower日志太短，以至于在冲突的位置没有Log条目，Leader应该回退到Follower最后一条Log条目的下一条
		if args.PrevLogIndex > lastLogIndex {
			//follower在对应位置没有Log，那么这里会返回 -1
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex - rf.getLastLog().Index
		} else { //follower的日志存在冲突
			//将自己的任期号放在XTerm中
			reply.XTerm = rf.logs[args.PrevLogIndex-firstLogIndex].Term
			i := 0
			//找到对应任期号为XTerm的第一条Log条目的槽位号
			for i = args.PrevLogIndex; i > firstLogIndex; i-- {
				if rf.logs[i-firstLogIndex].Term != reply.XTerm {
					break
				}
			}
			reply.XIndex = i + 1
		}
		return
	}
	// 没有发生丢失或冲突
	for i, entry := range args.Entries {
		//找到Index不合
		if entry.Index > lastLogIndex ||
			// term不合的地方
			rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			//直接覆盖后面的entry
			rf.logs = append(rf.logs[:entry.Index-firstLogIndex], args.Entries[i:]...)
		}
	}
	rf.followerCommit(args.LeaderCommit)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 确保当前仍是Leader状态并且Term没有改变的情况下才处理reply RPC
	if rf.state == Leader && rf.currentTerm == args.Term {
		// 不是leader了
		if reply.Term > rf.currentTerm {
			rf.changeState(Follower)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return
		}
		if reply.Success {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.leaderCommit()
		} else {
			// case 3: 缺少日志
			if reply.XTerm == -1 {
				rf.nextIndex[peer] -= reply.XLen
			} else {
				firstLogIndex := rf.getFirstLog().Index
				find := false
				for i := args.PrevLogIndex; i >= firstLogIndex; i-- {
					// case 2: Leader发现自己有任期的日志，它会将自己本地记录的nextIndex设置到本地在XTerm位置的Log条目后面
					if rf.logs[i-firstLogIndex].Term == reply.XIndex {
						find = true
						rf.nextIndex[peer] = reply.XIndex + 1
						break
					}
				}
				// case1 : Leader完全没有XTerm的任何Log，那么它应该回退到XIndex对应的位置
				if !find {
					rf.nextIndex[peer] = reply.XIndex
				}
			}
		}
	}
	DPrintf("[Node %v]'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} "+
		"after handling AppendEntriesReply %v from [Node %v] for AppendEntriesArgs %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, peer, args)
}

// append entries rpc
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) followerCommit(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		rf.commitIndex = Min(leaderCommit, rf.getLastLog().Index)
		rf.applyCond.Signal()
	}
}
func (rf *Raft) leaderCommit() {
	//注意：是根据matchIndex，判断出那些log已经被大多数peer记录了
	n := len(rf.matchIndex)
	tmp := make([]int, n)
	copy(tmp, rf.matchIndex)
	//降序排序
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	//找到新的commitIndex
	newCommitIndex := tmp[n/2]
	if newCommitIndex > rf.commitIndex {
		// leader只能提交当前任期下的日志
		if newCommitIndex <= rf.getLastLog().Index && rf.currentTerm == rf.logs[newCommitIndex-rf.getFirstLog().Index].Term {
			DPrintf("[Node %d] advance commitIndex from %d to %d with matchIndex %v in term %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			DPrintf("[Node %d] can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		lastLog := rf.getLastLog()
		newLog := Entry{
			// 无语，漏了个+1，卡了半天
			Index:   lastLog.Index + 1,
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.logs = append(rf.logs, newLog)
		//先改自己的
		rf.matchIndex[rf.me] = newLog.Index
		rf.nextIndex[rf.me] = newLog.Index + 1
		rf.persist()
		DPrintf("[Node %v] receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
		rf.broadcastHeartbeat(false)
		return newLog.Index, newLog.Term, true
	}
	return -1, -1, false
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		lastApplied := rf.lastApplied
		firstLogIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		applyEntries := make([]Entry, commitIndex-lastApplied)
		copy(applyEntries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		for _, entry := range applyEntries {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// ticker 协程会定期收到两个 timer 的到期事件
// 如果是 election timer 到期，则发起一轮选举
// 如果是 heartbeat timer 到期且节点是 leader，则发起一轮心跳
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("[Node %d] election elapsed, start election", rf.me)
			rf.changeState(Candidate)
			rf.currentTerm += 1
			rf.startElection()
			//	startElection异步发起投票后返回，重置选举超时时间
			rf.electionTimer.Reset(RandomElectionTimeOut())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// 只有leader才会心跳
			if rf.state == Leader {
				DPrintf("[Leader %d] Heartbeat elapsed, start new Heartbeat", rf.me)
				rf.broadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(FixedHeartBeatTimeout())
			}
			rf.mu.Unlock()

		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.doReplicate(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		currentTerm: 0,
		logs:        make([]Entry, 1),
		votedFor:    -1,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		state:          Follower,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		electionTimer:  time.NewTimer(RandomElectionTimeOut()),
		heartbeatTimer: time.NewTimer(FixedHeartBeatTimeout()),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// 启动复制器 goroutine 以批量复制条目
			// 分别管理对应 peer 的复制状态
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	// 用来触发 heartbeat timeout 和 election timeout
	go rf.ticker()
	// 用来往 applyCh 中 push 提交的日志并保证 exactly once
	go rf.applier()
	return rf
}
