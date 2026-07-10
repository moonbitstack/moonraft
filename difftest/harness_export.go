// This file is NOT part of upstream etcd raft. It is added to the local copy
// only to let an out-of-package differential-testing harness (1) pin the
// randomized election timeout for determinism and (2) read the authoritative
// internal state that is otherwise private. Behaviour of the algorithm is not
// touched.

package raft

import "go.etcd.io/raft/v3/quorum"

// HarnessSetRandomizedElectionTimeout pins randomizedElectionTimeout so two
// implementations with different RNGs campaign on the same tick.
func HarnessSetRandomizedElectionTimeout(rn *RawNode, v int) {
	rn.raft.randomizedElectionTimeout = v
}

// HarnessSnapshot is a normalized, comparable view of one node's authoritative
// state, assembled from fields that are private to package raft.
type HarnessSnapshot struct {
	Role             string
	Term             uint64
	Vote             uint64
	Commit           uint64
	Applied          uint64
	LastIndex        uint64
	LastTerm         uint64
	Lead             uint64
	LeadTransferee   uint64
	PendingConfIndex uint64
	UncommittedSize  uint64
	Voters           []uint64
	VotersOutgoing   []uint64
	Learners         []uint64
	LearnersNext     []uint64
}

func mapSlice(m map[uint64]struct{}) []uint64 {
	if len(m) == 0 {
		return []uint64{}
	}
	return quorum.MajorityConfig(m).Slice()
}

// HarnessSnapshotOf reads the private state of a RawNode's raft.
func HarnessSnapshotOf(rn *RawNode) HarnessSnapshot {
	r := rn.raft
	li := r.raftLog.lastIndex()
	lt, _ := r.raftLog.term(li)
	cfg := r.trk.Config
	return HarnessSnapshot{
		Role:             r.state.String(),
		Term:             r.Term,
		Vote:             r.Vote,
		Commit:           r.raftLog.committed,
		Applied:          r.raftLog.applied,
		LastIndex:        li,
		LastTerm:         lt,
		Lead:             r.lead,
		LeadTransferee:   r.leadTransferee,
		PendingConfIndex: r.pendingConfIndex,
		UncommittedSize:  uint64(r.uncommittedSize),
		Voters:           cfg.Voters[0].Slice(),
		VotersOutgoing:   cfg.Voters[1].Slice(),
		Learners:         mapSlice(cfg.Learners),
		LearnersNext:     mapSlice(cfg.LearnersNext),
	}
}
