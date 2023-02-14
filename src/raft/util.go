package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// 判断哪种节点
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	log.Fatalf("unexpected NodeState %d", s)
	return ""
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry Entry) String() string {
	return fmt.Sprintf("{Index:%v,Term:%v}", entry.Index, entry.Term)
}

type lockerRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockerRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockerRand{
	rand: rand.New(rand.NewSource(time.Now().Unix())),
}

// Timeout
const (
	ElectionTimeOut  = 1000
	HeartbeatTimeOut = 125
)

func RandomElectionTimeOut() time.Duration {
	// 1000 ~ 2000 ms
	return time.Duration(ElectionTimeOut+globalRand.Intn(ElectionTimeOut)) * time.Millisecond
}

func FixedHeartBeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeOut) * time.Millisecond
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
