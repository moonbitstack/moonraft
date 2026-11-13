package main

import (
	"fmt"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// each scenario starts a fresh cluster, so a failure in one does not colour the
// next. They differ in who is given the shorter election timeout, and in which
// faults are injected.
type scenario struct {
	name string
	run  func(c *cluster, report func(string, ...interface{})) error
}

func scenarios() []scenario {
	return []scenario{
		{"election", peerLeads},
		{"replication", goLeads},
		{"failover", failover},
		{"snapshot", snapshotCatchUp},
	}
}

func reportf(report func(string, ...interface{}), format string, args ...interface{}) {
	report(format, args...)
}

// peerLeads: the MoonBit node campaigns first, wins, and its entries reach the
// etcd-io/raft voters.
func peerLeads(c *cluster, report func(string, ...interface{})) error {
	leader, err := c.waitForLeader(400)
	if err != nil {
		return err
	}
	if leader != c.peerID {
		return fmt.Errorf("the etcd node %d won the election the MoonBit node should have won", leader)
	}
	reportf(report, "leader: MoonBit node %d (term %d)", leader, mustStatus(c, leader).term)
	for i := 0; i < 5; i++ {
		if err := c.propose(fmt.Sprintf("set x=%d", i)); err != nil {
			return err
		}
	}
	if err := c.waitForCommit(6, 400); err != nil {
		return err
	}
	compared, last, err := c.agree()
	if err != nil {
		return err
	}
	if compared < 6 {
		return fmt.Errorf("only %d entries were comparable, wanted 6", compared)
	}
	reportf(report, "committed through %d on all three voters, %d entries identical", last, compared)
	return nil
}

// goLeads: an etcd-io/raft node campaigns first, wins, and the MoonBit node
// follows it — replication in the other direction.
func goLeads(c *cluster, report func(string, ...interface{})) error {
	leader, err := c.waitForLeader(400)
	if err != nil {
		return err
	}
	if leader == c.peerID {
		return fmt.Errorf("the MoonBit node won an election the etcd nodes should have won")
	}
	reportf(report, "leader: etcd node %d (term %d)", leader, mustStatus(c, leader).term)
	for i := 0; i < 5; i++ {
		if err := c.propose(fmt.Sprintf("set y=%d", i)); err != nil {
			return err
		}
	}
	if err := c.waitForCommit(6, 400); err != nil {
		return err
	}
	compared, last, err := c.agree()
	if err != nil {
		return err
	}
	if compared < 6 {
		return fmt.Errorf("only %d entries were comparable, wanted 6", compared)
	}
	reportf(report, "the MoonBit follower applied through %d, %d entries identical", last, compared)
	return nil
}

// failover: the MoonBit leader is killed, the etcd nodes elect a new leader and
// commit more entries, and the MoonBit node is restarted with an empty log and
// must be repaired from the new leader.
func failover(c *cluster, report func(string, ...interface{})) error {
	leader, err := c.waitForLeader(400)
	if err != nil {
		return err
	}
	if leader != c.peerID {
		return fmt.Errorf("the MoonBit node did not win the first election")
	}
	for i := 0; i < 2; i++ {
		if err := c.propose(fmt.Sprintf("before crash %d", i)); err != nil {
			return err
		}
	}
	if err := c.waitForCommit(3, 400); err != nil {
		return err
	}
	reportf(report, "MoonBit leader committed through 3, then crashed")

	if err := c.stopPeer(); err != nil {
		return err
	}
	newLeader, err := c.waitForLeader(400)
	if err != nil {
		return err
	}
	if newLeader == c.peerID {
		return fmt.Errorf("the crashed node is still named leader")
	}
	for i := 0; i < 3; i++ {
		if err := c.propose(fmt.Sprintf("after crash %d", i)); err != nil {
			return err
		}
	}
	after := mustStatus(c, newLeader)
	if err := c.waitForCommit(after.lastIndex, 400); err != nil {
		return err
	}
	target := mustStatus(c, newLeader)
	reportf(report, "etcd node %d took over in term %d and committed through %d", newLeader, target.term, target.commit)

	if err := c.startPeer(); err != nil {
		return err
	}
	for i := 0; i < 600; i++ {
		st, err := c.statusOf(c.peerID)
		if err != nil {
			return err
		}
		if st.commit >= target.commit && st.lastIndex >= target.lastIndex {
			break
		}
		if err := c.tick(); err != nil {
			return err
		}
		if i == 599 {
			return fmt.Errorf("the restarted node did not catch up: %+v vs %+v", st, target)
		}
	}
	restarted := mustStatus(c, c.peerID)
	if restarted.commit != target.commit {
		return fmt.Errorf("the restarted node committed through %d, the leader through %d", restarted.commit, target.commit)
	}
	compared, _, err := c.agree()
	if err != nil {
		return err
	}
	reportf(report, "restarted with an empty log and repaired through %d, %d entries identical", restarted.commit, compared)
	return nil
}

// snapshotCatchUp: while the MoonBit node is down the leader compacts its log;
// a node that comes back with an empty log can then only be brought up to date
// by a snapshot, which the MoonBit side installs and continues from.
func snapshotCatchUp(c *cluster, report func(string, ...interface{})) error {
	leader, err := c.waitForLeader(400)
	if err != nil {
		return err
	}
	if leader == c.peerID {
		return fmt.Errorf("the MoonBit node won an election the etcd nodes should have won")
	}
	for i := 0; i < 10; i++ {
		if err := c.propose(fmt.Sprintf("early %d", i)); err != nil {
			return err
		}
	}
	if err := c.waitForCommit(11, 400); err != nil {
		return err
	}
	if err := c.stopPeer(); err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		if err := c.propose(fmt.Sprintf("late %d", i)); err != nil {
			return err
		}
	}
	target := mustStatus(c, leader)
	if err := c.waitForCommit(target.lastIndex, 400); err != nil {
		return err
	}
	target = mustStatus(c, leader)
	payload := fmt.Sprintf("state through index %d", target.commit)
	node := c.goNodes[leader]
	conf := &pb.ConfState{Voters: c.voters}
	if _, err := node.storage.CreateSnapshot(target.commit, conf, []byte(payload)); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	if err := node.storage.Compact(target.commit); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	reportf(report, "leader compacted its log through %d (first index %d)", target.commit, mustFirstIndex(node))

	if err := c.startPeer(); err != nil {
		return err
	}
	var snap snapshot
	for i := 0; i < 600; i++ {
		st, err := c.statusOf(c.peerID)
		if err != nil {
			return err
		}
		if st.commit >= target.commit && st.lastIndex >= target.lastIndex {
			snap, err = c.snapshotOfPeer()
			if err != nil {
				return err
			}
			break
		}
		if err := c.tick(); err != nil {
			return err
		}
		if i == 599 {
			return fmt.Errorf("the node did not catch up from a snapshot: %+v", st)
		}
	}
	if snap.index < target.commit {
		return fmt.Errorf("the node restored from a snapshot at %d, the leader compacted through %d", snap.index, target.commit)
	}
	if snap.data != hexOrDash([]byte(payload)) {
		return fmt.Errorf("the snapshot carried %q, wanted %q", snap.data, hexOrDash([]byte(payload)))
	}
	// A snapshot is only a starting point: the follower must keep replicating
	// ordinary entries from the new baseline, or the install merely reset it.
	for i := 0; i < 2; i++ {
		if err := c.propose(fmt.Sprintf("after snapshot %d", i)); err != nil {
			return err
		}
	}
	if err := c.waitForCommit(target.commit+2, 400); err != nil {
		return err
	}
	entries, err := c.entriesOf(c.peerID)
	if err != nil {
		return err
	}
	if len(entries) < 2 || entries[0].index != snap.index+1 {
		return fmt.Errorf("the node holds %d entries from the snapshot, wanted at least 2 from %d", len(entries), snap.index+1)
	}
	compared, _, err := c.agree()
	if err != nil {
		return err
	}
	reportf(report, "installed the snapshot at %d and replicated entries %d..%d after it, %d entries identical",
		snap.index, entries[0].index, entries[len(entries)-1].index, compared)
	return nil
}

func mustStatus(c *cluster, id uint64) status {
	st, err := c.statusOf(id)
	if err != nil {
		panic(err)
	}
	return st
}

func mustFirstIndex(node *goNode) uint64 {
	first, err := node.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return first
}

var _ = raft.IsEmptyHardState
var _ = strings.TrimSpace
