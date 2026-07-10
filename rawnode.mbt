// Copyright 2015 The etcd Authors
// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
/// This server's soft state — its known leader and current role (etcd's
/// `raft.softState`). Reading it never disturbs the protocol.
pub fn RaftNode::soft_state(self : RaftNode) -> SoftState {
  { lead: self.leader(), state: self.role() }
}

///|
/// The goroutine-free driver of a Raft server (etcd's `RawNode`). The core
/// `RaftNode` produces its output — messages to send, entries appended,
/// commits — as return values of `tick`/`step`/`propose`; `RawNode` batches
/// that output into the synchronous Ready/Advance cycle a real deployment runs:
/// take a `Ready`, persist its entries and hard state, send its messages, apply
/// its committed entries, then call `advance`.
///
/// It needs no language-level `async`: a `Ready` is a plain value and `advance`
/// a plain call. The one piece of bookkeeping the core does not track is which
/// log entries the application has already persisted; `stabled` is that
/// watermark, and it is what separates entries still to be written (`entries`)
/// from committed entries ready to apply (`committed_entries`).
pub struct RawNode {
  raft : RaftNode
  msgs : Array[Message]
  read_states : Array[ReadState]
  mut prev_soft : SoftState
  mut prev_hard : HardState
  mut stabled : UInt64
}

///|
/// Build a driver over `raft` (etcd's `NewRawNode`). The previous soft and hard
/// states are seeded from the server as it stands, so the first `Ready` reports
/// a change only if one genuinely happens afterwards. Whatever is already in the
/// log counts as durably stored — on a restart the recovered log is stable, and
/// only newly appended entries need writing — so `stabled` starts at the last
/// log index.
pub fn RawNode::new(raft : RaftNode) -> RawNode {
  {
    raft,
    msgs: [],
    read_states: [],
    prev_soft: raft.soft_state(),
    prev_hard: raft.node().hard_state(),
    stabled: raft.node().last_log_index(),
  }
}

///|
/// View this server through the Ready/Advance contract. The two are the same
/// server; the `RawNode` only batches the core's output into the cycle a real
/// deployment drives.
pub fn RaftNode::raw(self : RaftNode) -> RawNode {
  RawNode::new(self)
}

///|
/// The underlying core server, for reading protocol state in tests and drivers.
pub fn RawNode::node(self : RawNode) -> RaftNode {
  self.raft
}

///|
/// Advance the logical clock by one tick, buffering any messages the tick emits
/// (a leader's heartbeats, or a follower's campaign) for the next `Ready`.
pub fn RawNode::tick(self : RawNode) -> Unit {
  for m in self.raft.tick() {
    self.msgs.push(m)
  }
}

///|
/// Start a campaign, buffering the vote (or pre-vote) requests it emits.
pub fn RawNode::campaign(self : RawNode) -> Unit {
  for m in self.raft.campaign() {
    self.msgs.push(m)
  }
}

///|
/// Propose a client command. On a leader it is appended and the resulting
/// AppendEntries are buffered; on a non-leader it is dropped (this build does
/// not forward proposals — see PORTING.md).
pub fn RawNode::propose(self : RawNode, data : Bytes) -> Unit {
  for m in self.raft.propose(data) {
    self.msgs.push(m)
  }
}

///|
/// Propose a configuration change, appended and replicated like any entry so
/// every server folds it in at the same log position once committed (§6).
pub fn RawNode::propose_conf(self : RawNode, cc : ConfChange) -> Unit {
  for m in self.raft.propose_conf(cc) {
    self.msgs.push(m)
  }
}

///|
/// Feed one received message to the core, buffering the replies it produces.
pub fn RawNode::step(self : RawNode, msg : Message) -> Unit {
  for m in self.raft.step(msg) {
    self.msgs.push(m)
  }
}

///|
/// Request a linearizable read (etcd's `ReadIndex`). A leader that has committed
/// an entry in its term and still holds quorum records a `ReadState` at its
/// commit index, surfaced in the next `Ready`; a node that cannot answer records
/// nothing (this build does not forward reads to the leader — see PORTING.md).
pub fn RawNode::read_index(self : RawNode, rctx : Bytes) -> Unit {
  match self.raft.read_index() {
    Some(idx) => self.read_states.push({ index: idx, request_ctx: rctx })
    None => ()
  }
}

///|
/// Apply a committed configuration change to the local configuration and report
/// the resulting voter set (etcd's `ApplyConfChange`).
pub fn RawNode::apply_conf_change(
  self : RawNode,
  cc : ConfChange,
) -> Array[String] {
  cc.apply_to(self.raft.config)
  self.raft.config.voters()
}

///|
/// Begin transferring leadership to `target` (etcd's `TransferLeader`).
pub fn RawNode::transfer_leader(self : RawNode, target : String) -> Unit {
  for m in self.raft.transfer_leadership(target) {
    self.msgs.push(m)
  }
}

///|
/// The entries appended but not yet persisted by the application — everything
/// past the `stabled` watermark (etcd's `nextUnstableEnts`).
fn RawNode::next_unstable(self : RawNode) -> Array[Entry] {
  self.raft.node().entries_after(self.stabled)
}

///|
/// The committed-but-unapplied entries ready to hand to the state machine
/// (etcd's `nextCommittedEnts`): those in `(applied, min(commit, stabled)]`.
/// Capping at `stabled` enforces the contract that an entry is applied only
/// after it has been persisted, which is what makes a freshly appended entry
/// surface as `entries` in one `Ready` and as `committed_entries` in the next.
fn RawNode::next_committed(self : RawNode) -> Array[Entry] {
  let node = self.raft.node()
  let hi = if node.commit_index < self.stabled {
    node.commit_index
  } else {
    self.stabled
  }
  let out : Array[Entry] = []
  let mut i = node.last_applied + 1
  while i <= hi {
    match node.entry_at(i) {
      Some(e) => out.push(e)
      None => ()
    }
    i = i + 1
  }
  out
}

///|
/// Assemble a `Ready` without committing to handle it (etcd's
/// `readyWithoutAccept`): a pure read that leaves the buffered messages, read
/// states, and cursors untouched, so a caller may inspect pending work and
/// decide not to consume it.
pub fn RawNode::ready_without_accept(self : RawNode) -> Ready {
  let node = self.raft.node()
  let entries = self.next_unstable()
  let committed = self.next_committed()
  let soft = self.raft.soft_state()
  let hard = node.hard_state()
  {
    soft_state: if soft == self.prev_soft {
      None
    } else {
      Some(soft)
    },
    hard_state: if hard == self.prev_hard {
      None
    } else {
      Some(hard)
    },
    read_states: self.read_states.copy(),
    entries,
    snapshot: None,
    committed_entries: committed,
    messages: self.msgs.copy(),
    must_sync: must_sync(hard, self.prev_hard, entries.length()),
  }
}

///|
/// Record that the caller has taken the given `Ready` and will handle it
/// (etcd's `acceptReady`): adopt the reported soft/hard states as the new
/// baseline, drop the served read states, and clear the drained message buffer.
/// The stable/applied watermarks move in `advance`, not here.
fn RawNode::accept_ready(self : RawNode, rd : Ready) -> Unit {
  match rd.soft_state {
    Some(s) => self.prev_soft = s
    None => ()
  }
  match rd.hard_state {
    Some(h) => self.prev_hard = h
    None => ()
  }
  if rd.read_states.length() != 0 {
    self.read_states.clear()
  }
  self.msgs.clear()
}

///|
/// Take the outstanding work and commit to handling it (etcd's `Ready`): the
/// same batch as `ready_without_accept`, but the messages are now drained and
/// the soft/hard baseline advanced, so the next `Ready` reports only new work.
/// The returned batch *must* be handled and passed back to `advance`.
pub fn RawNode::ready(self : RawNode) -> Ready {
  let rd = self.ready_without_accept()
  self.accept_ready(rd)
  rd
}

///|
/// Whether any work is outstanding (etcd's `HasReady`): a soft- or hard-state
/// change, buffered messages, entries to persist or apply, or pending read
/// states. Lets a driver skip building a `Ready` when the node is idle.
pub fn RawNode::has_ready(self : RawNode) -> Bool {
  if self.raft.soft_state() != self.prev_soft {
    return true
  }
  let hard = self.raft.node().hard_state()
  if !hard.is_empty() && hard != self.prev_hard {
    return true
  }
  if self.msgs.length() > 0 {
    return true
  }
  if self.next_unstable().length() > 0 {
    return true
  }
  if self.next_committed().length() > 0 {
    return true
  }
  if self.read_states.length() > 0 {
    return true
  }
  false
}

///|
/// Notify the driver that the last `Ready` has been persisted and applied
/// (etcd's `Advance`): move the stable watermark past the entries that were
/// written, and the applied cursor past the entries that were applied.
pub fn RawNode::advance(self : RawNode, rd : Ready) -> Unit {
  let n = rd.entries.length()
  if n != 0 {
    self.stabled = rd.entries[n - 1].index
  }
  let c = rd.committed_entries.length()
  if c != 0 {
    self.raft.node().mark_applied(rd.committed_entries[c - 1].index)
  }
}

///|
/// This server's status snapshot (etcd's `Status`/`BasicStatus`).
pub fn RawNode::status(self : RawNode) -> RaftStatus {
  self.raft.status()
}

///|
/// Drive the node to quiescence for a caller that owns message delivery: keep
/// taking a `Ready`, treat its entries as persisted and its committed entries as
/// applied via `advance`, and collect every outbound message, until no work
/// remains. Returns the messages a transport would deliver. This is the plain,
/// synchronous read of the Ready/Advance loop that single-node drivers and the
/// browser demo run in place of a goroutine.
pub fn RawNode::stabilize(self : RawNode) -> Array[Message] {
  let out : Array[Message] = []
  while self.has_ready() {
    let rd = self.ready()
    for m in rd.messages {
      out.push(m)
    }
    self.advance(rd)
  }
  out
}
