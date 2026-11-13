package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// logEntry is what a node has committed, in the form both sides can produce.
type logEntry struct {
	index uint64
	term  uint64
	kind  uint8
	data  string
}

type status struct {
	role      string
	term      uint64
	commit    uint64
	leader    uint64 // 0 when there is none
	lastIndex uint64
}

type goNode struct {
	id        uint64
	raw       *raft.RawNode
	storage   *raft.MemoryStorage
	applied   []logEntry
	lastIndex uint64
}

type quietLogger struct{}

func (quietLogger) Debug(v ...interface{})                   {}
func (quietLogger) Debugf(format string, v ...interface{})   {}
func (quietLogger) Error(v ...interface{})                   {}
func (quietLogger) Errorf(format string, v ...interface{})   {}
func (quietLogger) Info(v ...interface{})                    {}
func (quietLogger) Infof(format string, v ...interface{})    {}
func (quietLogger) Warning(v ...interface{})                 {}
func (quietLogger) Warningf(format string, v ...interface{}) {}
func (quietLogger) Fatal(v ...interface{})                   { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(format string, v ...interface{})   { panic(fmt.Sprintf(format, v...)) }
func (quietLogger) Panic(v ...interface{})                   { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(format string, v ...interface{})   { panic(fmt.Sprintf(format, v...)) }

// newGoNode starts an etcd-io/raft node whose membership is the cluster's and
// whose log is empty, the state a fresh server is in: the ConfState rides in the
// storage's initial snapshot and no entry exists yet.
func newGoNode(id uint64, voters []uint64, electionTick, heartbeatTick int) (*goNode, error) {
	storage := raft.NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{Voters: voters}},
	}); err != nil {
		return nil, err
	}
	raw, err := raft.NewRawNode(&raft.Config{
		ID:              id,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		Logger:          quietLogger{},
	})
	if err != nil {
		return nil, err
	}
	return &goNode{id: id, raw: raw, storage: storage}, nil
}

// cluster is the network: three voters, two of them etcd-io/raft nodes in this
// process and one a MoonBit process on a pipe. Time is a tick, delivery is
// synchronous, and every message is routed by its `to` field.
type cluster struct {
	peerID        uint64
	voters        []uint64
	goNodes       map[uint64]*goNode
	peer          *peerProcess
	peerExe       string
	peerArgs      []string
	queue         []*pb.Message
	clock         int
	verbose       bool
	peersElection int
	goElection    int
}

func newCluster(peerExe string, peerElection, goElection int, verbose bool) (*cluster, error) {
	c := &cluster{
		peerID:        3,
		voters:        []uint64{1, 2, 3},
		goNodes:       map[uint64]*goNode{},
		peerExe:       peerExe,
		verbose:       verbose,
		peersElection: peerElection,
		goElection:    goElection,
	}
	for _, id := range []uint64{1, 2} {
		node, err := newGoNode(id, c.voters, goElection, 1)
		if err != nil {
			return nil, err
		}
		c.goNodes[id] = node
	}
	peers := []uint64{1, 2}
	peer, err := startPeer(peerExe, c.peerID, peers, peerElection, 1)
	if err != nil {
		return nil, err
	}
	c.peer = peer
	return c, nil
}

func (c *cluster) startPeer() error {
	peer, err := startPeer(c.peerExe, c.peerID, []uint64{1, 2}, c.peersElection, 1)
	if err != nil {
		return err
	}
	c.peer = peer
	return nil
}

func (c *cluster) stopPeer() error {
	if c.peer == nil {
		return nil
	}
	err := c.peer.stop()
	c.peer = nil
	return err
}

func (c *cluster) close() {
	if c.peer != nil {
		_ = c.peer.stop()
		c.peer = nil
	}
}

// drain collects a Go node's outgoing messages and records what it applied.
func (c *cluster) drain(node *goNode) error {
	for node.raw.HasReady() {
		ready := node.raw.Ready()
		if ready.HardState != nil {
			if err := node.storage.SetHardState(ready.HardState); err != nil {
				return err
			}
		}
		if len(ready.Entries) > 0 {
			if err := node.storage.Append(ready.Entries); err != nil {
				return err
			}
			node.lastIndex = ready.Entries[len(ready.Entries)-1].GetIndex()
		}
		if ready.Snapshot != nil {
			if err := node.storage.ApplySnapshot(ready.Snapshot); err != nil {
				return err
			}
			node.lastIndex = ready.Snapshot.GetMetadata().GetIndex()
		}
		for _, entry := range ready.CommittedEntries {
			node.applied = append(node.applied, logEntry{
				index: entry.GetIndex(),
				term:  entry.GetTerm(),
				kind:  uint8(entry.GetType()),
				data:  hexOrDash(entry.GetData()),
			})
			if entry.GetIndex() > node.lastIndex {
				node.lastIndex = entry.GetIndex()
			}
		}
		c.queue = append(c.queue, ready.Messages...)
		node.raw.Advance(ready)
	}
	return nil
}

// deliver hands every queued message to its destination, and keeps going until
// nothing is left: a reply produced while delivering joins the same queue.
func (c *cluster) deliver() error {
	for len(c.queue) > 0 {
		message := c.queue[0]
		c.queue = c.queue[1:]
		if message.GetTo() == c.peerID {
			if c.peer == nil {
				continue // the peer is down: the network drops its mail
			}
			line, err := encodeMessage(message)
			if err != nil {
				return err
			}
			if c.verbose {
				fmt.Printf("  %4d -> peer  %s\n", message.GetFrom(), line)
			}
			lines, err := c.peer.do(line)
			if err != nil {
				return err
			}
			for _, reply := range lines {
				if !strings.HasPrefix(reply, "msg ") {
					return fmt.Errorf("the peer answered a message with %q", reply)
				}
				decoded, err := decodeMessage(reply)
				if err != nil {
					return err
				}
				if c.verbose {
					fmt.Printf("  %4d <- peer  %s\n", decoded.GetTo(), reply)
				}
				c.queue = append(c.queue, decoded)
			}
			continue
		}
		node, ok := c.goNodes[message.GetTo()]
		if !ok {
			return fmt.Errorf("no node %d to deliver to", message.GetTo())
		}
		if c.verbose {
			fmt.Printf("  %4d -> %d     %v term=%d\n", message.GetFrom(), message.GetTo(), message.GetType(), message.GetTerm())
		}
		if err := node.raw.Step(message); err != nil {
			return err
		}
		if err := c.drain(node); err != nil {
			return err
		}
	}
	return nil
}

// tick advances every node by one tick and delivers what the ticks produced.
func (c *cluster) tick() error {
	c.clock++
	for _, node := range c.goNodes {
		node.raw.Tick()
		if err := c.drain(node); err != nil {
			return err
		}
	}
	if c.peer != nil {
		lines, err := c.peer.do("tick")
		if err != nil {
			return err
		}
		for _, line := range lines {
			decoded, err := decodeMessage(line)
			if err != nil {
				return err
			}
			c.queue = append(c.queue, decoded)
		}
	}
	return c.deliver()
}

func (c *cluster) statusOf(id uint64) (status, error) {
	if id == c.peerID {
		if c.peer == nil {
			return status{}, fmt.Errorf("the peer is down")
		}
		lines, err := c.peer.do("status")
		if err != nil {
			return status{}, err
		}
		if len(lines) != 1 {
			return status{}, fmt.Errorf("the peer answered status with %d lines", len(lines))
		}
		f := strings.Fields(lines[0])
		if len(f) != 6 || f[0] != "status" {
			return status{}, fmt.Errorf("unreadable status %q", lines[0])
		}
		term, err := number(f[2])
		if err != nil {
			return status{}, err
		}
		commit, err := number(f[3])
		if err != nil {
			return status{}, err
		}
		var leader uint64
		if f[4] != "-" {
			if leader, err = number(f[4]); err != nil {
				return status{}, err
			}
		}
		last, err := number(f[5])
		if err != nil {
			return status{}, err
		}
		return status{role: f[1], term: term, commit: commit, leader: leader, lastIndex: last}, nil
	}
	node, ok := c.goNodes[id]
	if !ok {
		return status{}, fmt.Errorf("no node %d", id)
	}
	basic := node.raw.BasicStatus()
	return status{
		role:      roleName(basic.RaftState),
		term:      basic.GetTerm(),
		commit:    basic.GetCommit(),
		leader:    basic.Lead,
		lastIndex: node.lastIndex,
	}, nil
}

// leader returns the one node every voter names as leader, or an error.
func (c *cluster) leader() (uint64, error) {
	var seen uint64
	for _, id := range c.voters {
		if id == c.peerID && c.peer == nil {
			continue // a node that is down names nobody
		}
		st, err := c.statusOf(id)
		if err != nil {
			return 0, err
		}
		if st.leader == 0 {
			return 0, fmt.Errorf("node %d names no leader", id)
		}
		if seen != 0 && st.leader != seen {
			return 0, fmt.Errorf("voters disagree: %d and %d", seen, st.leader)
		}
		seen = st.leader
	}
	if seen == c.peerID && c.peer == nil {
		return 0, fmt.Errorf("the voters still name the crashed node %d", seen)
	}
	if _, ok := c.goNodes[seen]; !ok && (c.peer == nil || seen != c.peerID) {
		return 0, fmt.Errorf("the voters name node %d, which is not in the cluster", seen)
	}
	return seen, nil
}

func (c *cluster) waitForLeader(maxTicks int) (uint64, error) {
	for i := 0; i < maxTicks; i++ {
		if err := c.tick(); err != nil {
			return 0, err
		}
		if leader, err := c.leader(); err == nil {
			return leader, nil
		}
	}
	return 0, fmt.Errorf("no leader after %d ticks", maxTicks)
}

func (c *cluster) waitForCommit(index uint64, maxTicks int) error {
	for i := 0; i < maxTicks; i++ {
		done := true
		for _, id := range c.voters {
			if id == c.peerID && c.peer == nil {
				continue
			}
			st, err := c.statusOf(id)
			if err != nil {
				return err
			}
			if st.commit < index {
				done = false
				break
			}
		}
		if done {
			return nil
		}
		if err := c.tick(); err != nil {
			return err
		}
	}
	return fmt.Errorf("commit index %d was not reached within %d ticks", index, maxTicks)
}

func (c *cluster) propose(data string) error {
	leader, err := c.leader()
	if err != nil {
		return err
	}
	if leader == c.peerID {
		lines, err := c.peer.do("propose " + hexOrDash([]byte(data)))
		if err != nil {
			return err
		}
		for _, line := range lines {
			decoded, err := decodeMessage(line)
			if err != nil {
				return err
			}
			c.queue = append(c.queue, decoded)
		}
		return c.deliver()
	}
	if err := c.goNodes[leader].raw.Propose([]byte(data)); err != nil {
		return err
	}
	return c.drain(c.goNodes[leader])
}

// entriesOf returns what a node has committed, in index order.
func (c *cluster) entriesOf(id uint64) ([]logEntry, error) {
	if id == c.peerID {
		if c.peer == nil {
			return nil, fmt.Errorf("the peer is down")
		}
		lines, err := c.peer.do("log")
		if err != nil {
			return nil, err
		}
		if len(lines) != 1 {
			return nil, fmt.Errorf("the peer answered log with %d lines", len(lines))
		}
		f := strings.Fields(lines[0])
		if len(f) < 6 || f[0] != "log" {
			return nil, fmt.Errorf("unreadable log %q", lines[0])
		}
		count, err := number(f[5])
		if err != nil {
			return nil, err
		}
		out := make([]logEntry, 0, count)
		for i := uint64(0); i < count; i++ {
			at := 6 + int(i)*4
			if at+3 >= len(f) {
				return nil, fmt.Errorf("the peer log ends early: %q", lines[0])
			}
			kind, err := number(f[at])
			if err != nil {
				return nil, err
			}
			term, err := number(f[at+1])
			if err != nil {
				return nil, err
			}
			index, err := number(f[at+2])
			if err != nil {
				return nil, err
			}
			out = append(out, logEntry{index: index, term: term, kind: uint8(kind), data: f[at+3]})
		}
		return out, nil
	}
	node := c.goNodes[id]
	out := append([]logEntry(nil), node.applied...)
	sort.Slice(out, func(i, j int) bool { return out[i].index < out[j].index })
	return out, nil
}

// agree checks that every voter committed the same entry at every index they
// share, and reports how many entries were compared.
func (c *cluster) agree() (int, uint64, error) {
	var first uint64 = ^uint64(0)
	var last uint64
	byNode := map[uint64]map[uint64]logEntry{}
	for _, id := range c.voters {
		if id == c.peerID && c.peer == nil {
			continue
		}
		entries, err := c.entriesOf(id)
		if err != nil {
			return 0, 0, err
		}
		m := map[uint64]logEntry{}
		for _, e := range entries {
			m[e.index] = e
		}
		if len(entries) > 0 {
			if entries[0].index < first {
				first = entries[0].index
			}
			if entries[len(entries)-1].index > last {
				last = entries[len(entries)-1].index
			}
		}
		byNode[id] = m
	}
	if first == ^uint64(0) {
		return 0, 0, nil
	}
	compared := 0
	for index := first; index <= last; index++ {
		var want logEntry
		var have bool
		for id, m := range byNode {
			entry, ok := m[index]
			if !ok {
				continue
			}
			if !have {
				want, have = entry, true
				continue
			}
			if entry.term != want.term || entry.data != want.data || entry.kind != want.kind {
				return compared, index, fmt.Errorf("node %d holds (%d,%s) at %d, %d holds (%d,%s)",
					id, entry.term, entry.data, index, 0, want.term, want.data)
			}
		}
		if have {
			compared++
		}
	}
	return compared, last, nil
}

// snapshotOf reports the baseline a node restored its log from.
type snapshot struct {
	index uint64
	term  uint64
	data  string
}

func (c *cluster) snapshotOfPeer() (snapshot, error) {
	lines, err := c.peer.do("log")
	if err != nil {
		return snapshot{}, err
	}
	f := strings.Fields(lines[0])
	index, err := number(f[1])
	if err != nil {
		return snapshot{}, err
	}
	term, err := number(f[2])
	if err != nil {
		return snapshot{}, err
	}
	return snapshot{index: index, term: term, data: f[4]}, nil
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

func roleName(state raft.StateType) string {
	switch state {
	case raft.StateLeader:
		return "leader"
	case raft.StateCandidate:
		return "candidate"
	case raft.StatePreCandidate:
		return "precandidate"
	default:
		return "follower"
	}
}
