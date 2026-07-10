// Differential-testing harness (Go / etcd side).
//
// Drives etcd raft RawNodes through a small deterministic DSL and emits one
// normalized JSON frame per command on stdout. The MoonBit harness emits the
// exact same schema so the two streams can be diffed line by line. Elections
// are driven explicitly (campaign) so neither side depends on its RNG.
package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type node struct {
	id      uint64
	rn      *raft.RawNode
	storage *raft.MemoryStorage
	alive   bool
	applied uint64
}

type busMsg struct {
	from, to uint64
	m        *pb.Message
}

type cfgTemplate struct {
	electionTick, heartbeatTick     int
	preVote, checkQuorum            bool
	leaseBased                      bool
	maxSizePerMsg, maxInflightBytes uint64
	maxInflightMsgs                 int
	maxUncommitted                  uint64
}

type env struct {
	nodes   map[uint64]*node
	order   []uint64
	bus     []busMsg
	blocked map[[2]uint64]bool
	step    int
	cfg     cfgTemplate
	out     *bufio.Writer
}

const noLimit = ^uint64(0)

func newEnv() *env {
	return &env{
		nodes:   map[uint64]*node{},
		blocked: map[[2]uint64]bool{},
		cfg: cfgTemplate{
			electionTick: 10, heartbeatTick: 1,
			preVote: false, checkQuorum: false, leaseBased: false,
			maxSizePerMsg: noLimit, maxInflightMsgs: 256,
			maxInflightBytes: 0, maxUncommitted: 0,
		},
		out: bufio.NewWriter(os.Stdout),
	}
}

func (e *env) buildConfig(id uint64, st *raft.MemoryStorage) *raft.Config {
	ro := raft.ReadOnlySafe
	if e.cfg.leaseBased {
		ro = raft.ReadOnlyLeaseBased
	}
	return &raft.Config{
		ID:                        id,
		ElectionTick:              e.cfg.electionTick,
		HeartbeatTick:             e.cfg.heartbeatTick,
		Storage:                   st,
		MaxSizePerMsg:             e.cfg.maxSizePerMsg,
		MaxInflightMsgs:           e.cfg.maxInflightMsgs,
		MaxInflightBytes:          e.cfg.maxInflightBytes,
		MaxUncommittedEntriesSize: e.cfg.maxUncommitted,
		CheckQuorum:               e.cfg.checkQuorum,
		PreVote:                   e.cfg.preVote,
		ReadOnlyOption:            ro,
	}
}

func (e *env) addCluster(ids []uint64) {
	cs := &pb.ConfState{Voters: ids}
	for _, id := range ids {
		st := raft.NewMemoryStorage()
		st.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: cs}})
		rn, err := raft.NewRawNode(e.buildConfig(id, st))
		must(err)
		e.nodes[id] = &node{id: id, rn: rn, storage: st, alive: true}
		e.order = append(e.order, id)
	}
	sort.Slice(e.order, func(i, j int) bool { return e.order[i] < e.order[j] })
	e.pinTimeouts()
}

func (e *env) pinTimeouts() {
	for _, n := range e.nodes {
		raft.HarnessSetRandomizedElectionTimeout(n.rn, e.cfg.electionTick)
	}
}

// pump drains every alive node's Ready until quiescent, persisting state,
// applying committed conf changes, and pushing outbound messages onto the bus.
// It returns the messages produced during this invocation (for the frame).
func (e *env) pump() []*pb.Message {
	var produced []*pb.Message
	for progress := true; progress; {
		progress = false
		for _, id := range e.order {
			n := e.nodes[id]
			if !n.alive || !n.rn.HasReady() {
				continue
			}
			progress = true
			rd := n.rn.Ready()
			if rd.Snapshot != nil {
				n.storage.ApplySnapshot(rd.Snapshot)
			}
			if len(rd.Entries) > 0 {
				n.storage.Append(rd.Entries)
			}
			if rd.HardState != nil {
				n.storage.SetHardState(rd.HardState)
			}
			for _, m := range rd.Messages {
				produced = append(produced, m)
				e.route(id, m)
			}
			for _, ent := range rd.CommittedEntries {
				n.applied = ent.GetIndex()
				if ent.GetType() == pb.EntryConfChange {
					cc := &pb.ConfChange{}
					if proto.Unmarshal(ent.GetData(), cc) == nil {
						n.rn.ApplyConfChange(cc)
					}
				} else if ent.GetType() == pb.EntryConfChangeV2 {
					cc := &pb.ConfChangeV2{}
					if proto.Unmarshal(ent.GetData(), cc) == nil {
						n.rn.ApplyConfChange(cc)
					}
				}
			}
			n.rn.Advance(rd)
		}
	}
	e.pinTimeouts()
	return produced
}

func (e *env) route(from uint64, m *pb.Message) {
	to := m.GetTo()
	if e.blocked[[2]uint64{from, to}] || e.blocked[[2]uint64{to, from}] {
		return // partitioned: message lost
	}
	e.bus = append(e.bus, busMsg{from: from, to: to, m: m})
}

func (e *env) deliver(from, to uint64) {
	var rest []busMsg
	for _, bm := range e.bus {
		if bm.from == from && bm.to == to {
			if n, ok := e.nodes[bm.to]; ok && n.alive {
				n.rn.Step(bm.m)
			}
			continue
		}
		rest = append(rest, bm)
	}
	e.bus = rest
}

func (e *env) deliverAll() {
	pending := e.bus
	e.bus = nil
	for _, bm := range pending {
		if n, ok := e.nodes[bm.to]; ok && n.alive {
			n.rn.Step(bm.m)
		}
	}
}

func (e *env) drop(from, to uint64) {
	var rest []busMsg
	for _, bm := range e.bus {
		if bm.from == from && bm.to == to {
			continue
		}
		rest = append(rest, bm)
	}
	e.bus = rest
}

// ---- normalized schema ----

func roleName(s string) string {
	// StateFollower/StateCandidate/StateLeader/StatePreCandidate -> canonical.
	// PreCandidate is normalized to Candidate for cross-impl comparison because
	// the MoonBit port has no PreCandidate Role (it uses in_pre_campaign).
	switch s {
	case "StateFollower":
		return "Follower"
	case "StatePreCandidate":
		return "PreCandidate"
	case "StateCandidate":
		return "Candidate"
	case "StateLeader":
		return "Leader"
	}
	return s
}

func msgType(t pb.MessageType) string {
	// Map etcd message types to the canonical cross-impl tag set.
	switch t {
	case pb.MsgVote:
		return "Vote"
	case pb.MsgVoteResp:
		return "VoteResp"
	case pb.MsgPreVote:
		return "PreVote"
	case pb.MsgPreVoteResp:
		return "PreVoteResp"
	case pb.MsgApp:
		return "Append"
	case pb.MsgAppResp:
		return "AppendResp"
	case pb.MsgHeartbeat:
		return "Heartbeat"
	case pb.MsgHeartbeatResp:
		return "HeartbeatResp"
	case pb.MsgSnap:
		return "Snapshot"
	case pb.MsgTimeoutNow:
		return "TimeoutNow"
	case pb.MsgReadIndex:
		return "ReadIndex"
	case pb.MsgReadIndexResp:
		return "ReadIndexResp"
	case pb.MsgTransferLeader:
		return "TransferLeader"
	}
	return t.String()
}

func jstr(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func msgJSON(m *pb.Message) string {
	index, logterm, entries := m.GetIndex(), m.GetLogTerm(), len(m.GetEntries())
	if m.GetType() == pb.MsgSnap {
		// Make snapshot messages comparable: carry metadata index/term.
		index = m.GetSnapshot().GetMetadata().GetIndex()
		logterm = m.GetSnapshot().GetMetadata().GetTerm()
		entries = 0
	}
	return fmt.Sprintf(
		`{"type":%s,"from":%q,"to":%q,"term":%d,"index":%d,"logterm":%d,"entries":%d,"commit":%d,"reject":%t,"rejecthint":%d}`,
		jstr(msgType(m.GetType())),
		strconv.FormatUint(m.GetFrom(), 10), strconv.FormatUint(m.GetTo(), 10),
		m.GetTerm(), index, logterm, entries,
		m.GetCommit(), m.GetReject(), m.GetRejectHint())
}

func idsJSON(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Quote(strconv.FormatUint(id, 10))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func (e *env) nodeJSON(n *node) string {
	s := raft.HarnessSnapshotOf(n.rn)
	vote := ""
	if s.Vote != 0 {
		vote = strconv.FormatUint(s.Vote, 10)
	}
	lead := ""
	if s.Lead != 0 {
		lead = strconv.FormatUint(s.Lead, 10)
	}
	transferee := ""
	if s.LeadTransferee != 0 {
		transferee = strconv.FormatUint(s.LeadTransferee, 10)
	}
	return fmt.Sprintf(
		`{"id":%q,"role":%s,"term":%d,"vote":%q,"lead":%q,"commit":%d,"applied":%d,"last_index":%d,"last_term":%d,`+
			`"lead_transferee":%q,"pending_conf_index":%d,"uncommitted":%d,`+
			`"voters":%s,"voters_outgoing":%s,"learners":%s,"learners_next":%s}`,
		strconv.FormatUint(n.id, 10), jstr(roleName(s.Role)), s.Term, vote, lead,
		s.Commit, s.Applied, s.LastIndex, s.LastTerm,
		transferee, s.PendingConfIndex, s.UncommittedSize,
		idsJSON(s.Voters), idsJSON(s.VotersOutgoing), idsJSON(s.Learners), idsJSON(s.LearnersNext))
}

func (e *env) emit(cmd string, msgs []*pb.Message) {
	e.step++
	sort.Slice(msgs, func(i, j int) bool { return msgLess(msgs[i], msgs[j]) })
	var nb []string
	for _, id := range e.order {
		if n, ok := e.nodes[id]; ok {
			nb = append(nb, e.nodeJSON(n))
		}
	}
	var mb []string
	for _, m := range msgs {
		mb = append(mb, msgJSON(m))
	}
	fmt.Fprintf(e.out, `{"step":%d,"cmd":%s,"nodes":[%s],"msgs":[%s]}`+"\n",
		e.step, jstr(cmd), strings.Join(nb, ","), strings.Join(mb, ","))
}

// typeRank gives a fixed cross-impl ordering of message types so both harnesses
// sort identically (MoonBit's String "<" is length-first, not lexicographic).
func typeRank(t string) int {
	switch t {
	case "Append":
		return 0
	case "AppendResp":
		return 1
	case "ForgetLeader":
		return 2
	case "Heartbeat":
		return 3
	case "HeartbeatResp":
		return 4
	case "PreVote":
		return 5
	case "PreVoteResp":
		return 6
	case "Propose":
		return 7
	case "ReadIndex":
		return 8
	case "ReadIndexResp":
		return 9
	case "Snapshot":
		return 10
	case "SnapshotResp":
		return 11
	case "TimeoutNow":
		return 12
	case "TransferLeader":
		return 13
	case "Vote":
		return 14
	case "VoteResp":
		return 15
	}
	return 99
}

func msgLess(a, b *pb.Message) bool {
	ra, rb := typeRank(msgType(a.GetType())), typeRank(msgType(b.GetType()))
	if ra != rb {
		return ra < rb
	}
	if a.GetTo() != b.GetTo() {
		return a.GetTo() < b.GetTo()
	}
	if a.GetFrom() != b.GetFrom() {
		return a.GetFrom() < b.GetFrom()
	}
	if a.GetTerm() != b.GetTerm() {
		return a.GetTerm() < b.GetTerm()
	}
	if a.GetIndex() != b.GetIndex() {
		return a.GetIndex() < b.GetIndex()
	}
	if a.GetLogTerm() != b.GetLogTerm() {
		return a.GetLogTerm() < b.GetLogTerm()
	}
	return a.GetRejectHint() < b.GetRejectHint()
}

// ---- DSL ----

func (e *env) run(lines []string) {
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		cmd, args := f[0], f[1:]
		switch cmd {
		case "config":
			e.applyConfig(args)
		case "cluster":
			e.addCluster(parseIDs(args))
			e.emit(line, e.pump())
		case "campaign":
			e.nodes[u(args[0])].rn.Campaign()
			e.emit(line, e.pump())
		case "propose":
			data := []byte(strings.Join(args[1:], " "))
			e.nodes[u(args[0])].rn.Propose(data)
			e.emit(line, e.pump())
		case "conf_change":
			e.confChange(u(args[0]), args[1], u(args[2]))
			e.emit(line, e.pump())
		case "conf_leave":
			e.nodes[u(args[0])].rn.ProposeConfChange(&pb.ConfChangeV2{})
			e.emit(line, e.pump())
		case "tick":
			cnt := 1
			if len(args) > 1 {
				cnt, _ = strconv.Atoi(args[1])
			}
			e.tick(args[0], cnt)
			e.emit(line, e.pump())
		case "deliver":
			e.deliver(u(args[0]), u(args[1]))
			e.emit(line, e.pump())
		case "deliver_all":
			e.deliverAll()
			e.emit(line, e.pump())
		case "stabilize":
			e.emit(line, e.stabilize())
		case "drop":
			e.drop(u(args[0]), u(args[1]))
			e.emit(line, nil)
		case "partition":
			e.partition(args)
			e.emit(line, nil)
		case "heal":
			e.blocked = map[[2]uint64]bool{}
			e.emit(line, e.pump())
		case "crash":
			e.nodes[u(args[0])].alive = false
			e.emit(line, nil)
		case "transfer":
			e.nodes[u(args[0])].rn.TransferLeader(u(args[1]))
			e.emit(line, e.pump())
		case "read_index":
			e.nodes[u(args[0])].rn.ReadIndex([]byte(strings.Join(args[1:], " ")))
			e.emit(line, e.pump())
		case "report_unreachable":
			e.nodes[u(args[0])].rn.ReportUnreachable(u(args[1]))
			e.emit(line, e.pump())
		case "report_snapshot":
			st := raft.SnapshotFinish
			if len(args) > 2 && args[2] == "fail" {
				st = raft.SnapshotFailure
			}
			e.nodes[u(args[0])].rn.ReportSnapshot(u(args[1]), st)
			e.emit(line, e.pump())
		case "snapshot":
			e.snapshot(u(args[0]))
			e.emit(line, e.pump())
		default:
			fmt.Fprintf(os.Stderr, "unknown cmd: %s\n", cmd)
			os.Exit(2)
		}
	}
	e.out.Flush()
}

func (e *env) applyConfig(args []string) {
	for _, a := range args {
		kv := strings.SplitN(a, "=", 2)
		k := kv[0]
		v := ""
		if len(kv) > 1 {
			v = kv[1]
		}
		switch k {
		case "prevote":
			e.cfg.preVote = v == "true"
		case "checkquorum":
			e.cfg.checkQuorum = v == "true"
		case "leasebased":
			e.cfg.leaseBased = v == "true"
		case "electiontick":
			e.cfg.electionTick, _ = strconv.Atoi(v)
		case "heartbeattick":
			e.cfg.heartbeatTick, _ = strconv.Atoi(v)
		case "maxsizepermsg":
			e.cfg.maxSizePerMsg, _ = strconv.ParseUint(v, 10, 64)
		case "maxinflight":
			e.cfg.maxInflightMsgs, _ = strconv.Atoi(v)
		case "maxinflightbytes":
			e.cfg.maxInflightBytes, _ = strconv.ParseUint(v, 10, 64)
		case "maxuncommitted":
			e.cfg.maxUncommitted, _ = strconv.ParseUint(v, 10, 64)
		}
	}
}

func (e *env) tick(who string, cnt int) {
	for i := 0; i < cnt; i++ {
		if who == "all" {
			for _, id := range e.order {
				if e.nodes[id].alive {
					e.nodes[id].rn.Tick()
				}
			}
		} else {
			e.nodes[u(who)].rn.Tick()
		}
	}
}

func (e *env) confChange(id uint64, op string, target uint64) {
	var t pb.ConfChangeType
	switch op {
	case "addnode":
		t = pb.ConfChangeAddNode
	case "removenode":
		t = pb.ConfChangeRemoveNode
	case "addlearner":
		t = pb.ConfChangeAddLearnerNode
	}
	cc := &pb.ConfChange{Type: t.Enum(), NodeId: &target}
	e.nodes[id].rn.ProposeConfChange(cc)
}

func (e *env) stabilize() []*pb.Message {
	var all []*pb.Message
	for i := 0; i < 2000; i++ {
		all = append(all, e.pump()...)
		if len(e.bus) == 0 {
			break
		}
		e.deliverAll()
	}
	all = append(all, e.pump()...)
	return all
}

func (e *env) partition(args []string) {
	// partition g1 g2 : block every cross-group pair. groups are comma lists.
	if len(args) < 2 {
		return
	}
	g1 := parseIDs(strings.Split(args[0], ","))
	g2 := parseIDs(strings.Split(args[1], ","))
	for _, a := range g1 {
		for _, b := range g2 {
			e.blocked[[2]uint64{a, b}] = true
			e.blocked[[2]uint64{b, a}] = true
		}
	}
	// drop already-queued cross-group messages
	var rest []busMsg
	for _, bm := range e.bus {
		if e.blocked[[2]uint64{bm.from, bm.to}] {
			continue
		}
		rest = append(rest, bm)
	}
	e.bus = rest
}

func (e *env) snapshot(id uint64) {
	n := e.nodes[id]
	s := raft.HarnessSnapshotOf(n.rn)
	cs := &pb.ConfState{Voters: s.Voters, VotersOutgoing: s.VotersOutgoing, Learners: s.Learners, LearnersNext: s.LearnersNext}
	idx := n.applied
	if idx == 0 {
		idx = s.Commit
	}
	_, err := n.storage.CreateSnapshot(idx, cs, []byte("snap"))
	if err == nil {
		n.storage.Compact(idx)
	}
}

// ---- helpers ----

func parseIDs(ss []string) []uint64 {
	var out []uint64
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, u(s))
		}
	}
	return out
}

func u(s string) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	must(err)
	return v
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: harness <script>")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	must(err)
	e := newEnv()
	e.run(strings.Split(string(data), "\n"))
}
