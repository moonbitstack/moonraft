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
/// a plain call. The unstable-vs-stable split that separates entries still to be
/// written (`entries`) from committed entries ready to apply
/// (`committed_entries`) is kept by a real `RaftLog`: its `storage` holds what
/// the application has persisted, its `unstable` tail holds what has not.
pub struct RawNode {
  raft : RaftNode
  log : RaftLog
  msgs : Array[Message]
  read_states : Array[ReadState]
  mut prev_soft : SoftState
  mut prev_hard : HardState
  // When set (etcd's AsyncStorageWrites), a Ready hands its entries and committed
  // entries to local storage threads as directives rather than expecting the
  // caller to persist/apply them inline before `advance`.
  mut async_storage : Bool
}

///|
/// Build a driver over `raft` (etcd's `NewRawNode`). The previous soft and hard
/// states are seeded from the server as it stands, so the first `Ready` reports
/// a change only if one genuinely happens afterwards. The `RaftLog` is recovered
/// from the node's durable state: whatever is already in the node's log (and its
/// snapshot baseline) is loaded into stable storage, so only entries appended
/// afterwards land in the unstable tail and need writing.
pub fn RawNode::new(raft : RaftNode) -> RawNode {
  RawNode::new_sized(raft, no_limit)
}

///|
/// Build a driver that paginates each Ready's committed entries to at most
/// `max_committed_size` bytes (etcd's `MaxCommittedSizePerReady`). A burst of
/// commits is then handed to the application over several `Ready`s instead of
/// one unbounded batch.
pub fn RawNode::new_sized(
  raft : RaftNode,
  max_committed_size : UInt64,
) -> RawNode {
  let node = raft.node()
  let storage = MemoryStorage::new()
  if node.snapshot_index > 0 {
    // Seed the fresh storage's baseline directly: `apply_snapshot`'s out-of-date
    // guard is meaningless against an empty baseline, and its raising signature
    // would force a vacuous catch here.
    storage.seed_snapshot({
      last_index: node.snapshot_index,
      last_term: node.snapshot_term,
      data: b"",
      conf_state: ConfState::empty(),
    })
  }
  storage.append(node.entries_after(node.snapshot_index))
  let log = RaftLog::new_with_size(storage, max_committed_size)
  log.seed(node.commit_index, node.last_applied)
  {
    raft,
    log,
    msgs: [],
    read_states: [],
    prev_soft: raft.soft_state(),
    prev_hard: node.hard_state(),
    async_storage: false,
  }
}

///|
/// View this server through the Ready/Advance contract, paginating committed
/// entries to `max_committed_size` bytes per `Ready`.
pub fn RaftNode::raw_with_max_committed_size(
  self : RaftNode,
  max_committed_size : UInt64,
) -> RawNode {
  RawNode::new_sized(self, max_committed_size)
}

///|
/// View this server under AsyncStorageWrites (etcd's `Config.AsyncStorageWrites`):
/// each `Ready` hands its entries and committed entries to local storage threads
/// as `StorageAppend` / `StorageApply` directives, and the caller returns the
/// paired responses through `step_append_resp` / `step_apply_resp` once the work
/// is durable/applied. `advance` becomes a no-op; the responses drive the
/// stable/applied cursors instead.
pub fn RaftNode::raw_async(self : RaftNode) -> RawNode {
  let rn = self.raw()
  rn.async_storage = true
  rn
}

///|
/// As `raw_async`, but also paginating each `StorageApply` batch to
/// `max_committed_size` bytes (etcd's `AsyncStorageWrites` +
/// `MaxCommittedSizePerReady`).
pub fn RaftNode::raw_async_sized(
  self : RaftNode,
  max_committed_size : UInt64,
) -> RawNode {
  let rn = RawNode::new_sized(self, max_committed_size)
  rn.async_storage = true
  rn
}

///|
/// Whether this driver is in AsyncStorageWrites mode.
pub fn RawNode::is_async(self : RawNode) -> Bool {
  self.async_storage
}

///|
/// Reconcile the log with the consensus core: fold any entries the core has
/// appended (or a divergent suffix it rewrote) into the unstable tail, and pull
/// the commit index forward. The core keeps the authoritative log on `Node`;
/// this is where that truth flows into the unstable/stable split the driver
/// reports. Committed entries never change, so the scan starts just past them.
fn RawNode::sync_log(self : RawNode) -> Unit {
  let node = self.raft.node()
  let node_last = node.last_log_index()
  let mut i = self.log.committed() + 1
  let known_last = self.log.last_index()
  while i <= node_last && i <= known_last {
    if node.term_at(i) != self.log.zero_term_on_out_of_bounds(i) {
      break
    }
    i = i + 1
  }
  if i <= node_last {
    self.log.append(node.entries_after(i - 1)) |> ignore
  }
  let last = self.log.last_index()
  self.log.commit_to(
    if node.commit_index < last {
      node.commit_index
    } else {
      last
    },
  )
  // A read confirmed by the core (a lease read, or a safe read a heartbeat quorum
  // has now acknowledged) lives in the core's read-state buffer; fold it into the
  // driver's so it surfaces in the next Ready and is cleared on accept.
  for rs in self.raft.take_read_states() {
    self.read_states.push(rs)
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
/// AppendEntries are buffered; on a follower it is forwarded to the known leader
/// (etcd's MsgProp forwarding), or dropped if forwarding is disabled or no leader
/// is known; a candidate drops it.
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
/// Propose a batch (joint) configuration change, appended and replicated like
/// any entry so every server folds the same change in at the same log position
/// once it commits (etcd's `ProposeConfChange` with a `ConfChangeV2`).
pub fn RawNode::propose_conf_v2(self : RawNode, cc : ConfChangeV2) -> Unit {
  for m in self.raft.propose_conf_v2(cc) {
    self.msgs.push(m)
  }
}

///|
/// The current membership as a `ConfState` (etcd's `ApplyConfChange` return /
/// `ConfState()`): the voters, learners, the outgoing half while joint, the
/// staged-demotion `learners_next`, and whether an auto-leave is pending.
pub fn RawNode::conf_state(self : RawNode) -> ConfState {
  self.raft.conf_state()
}

///|
/// Feed one received message to the core, buffering the replies it produces.
pub fn RawNode::step(self : RawNode, msg : Message) -> Unit {
  for m in self.raft.step(msg) {
    self.msgs.push(m)
  }
}

///|
/// Request a linearizable read (etcd's `ReadIndex`, which steps a `MsgReadIndex`).
/// Under `ReadOnlySafe` the read is not answered from the leader's lease but
/// confirmed by a fresh heartbeat quorum, so the confirming heartbeats are
/// buffered for the next `Ready`; the read surfaces once a quorum acknowledges the
/// position (drained from the core in `sync_log`). The synchronous lease accessor
/// `RaftNode::read_index` was wrong here: it returns an index without emitting the
/// confirmation round, so under `ReadOnlySafe` the read was never confirmed.
pub fn RawNode::read_index(self : RawNode, rctx : Bytes) -> Unit {
  for m in self.raft.request_read_index(rctx) {
    self.msgs.push(m)
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
/// Voluntarily forget the current leader so this node can start an election
/// without waiting out the election timeout (etcd's `ForgetLeader`).
pub fn RawNode::forget_leader(self : RawNode) -> Unit {
  self.raft.forget_leader()
}

///|
/// Report that a message to `id` could not be delivered (etcd's
/// `ReportUnreachable`): the leader stops streaming to that follower until it
/// responds again.
pub fn RawNode::report_unreachable(self : RawNode, id : String) -> Unit {
  self.raft.report_unreachable(id)
}

///|
/// Report the outcome of a snapshot sent to `id` (etcd's `ReportSnapshot`):
/// `reject` true means the follower could not apply it, so the leader retries.
pub fn RawNode::report_snapshot(
  self : RawNode,
  id : String,
  reject : Bool,
) -> Unit {
  self.raft.report_snapshot(id, reject)
}

///|
/// This server's basic status (etcd's `BasicStatus`): id, term, vote, commit,
/// leader and role, without the per-follower progress map.
pub fn RawNode::basic_status(self : RawNode) -> RaftStatus {
  self.raft.status()
}

///|
/// This server's full status (etcd's `Status`): basic status plus the
/// per-follower progress view and the current configuration.
pub fn RawNode::full_status(self : RawNode) -> FullStatus {
  self.raft.full_status()
}

///|
/// Visit each follower's progress (etcd's `WithProgress`).
pub fn RawNode::with_progress(
  self : RawNode,
  visit : (String, ProgressStatus) -> Unit,
) -> Unit {
  self.raft.with_progress(visit)
}

///|
/// The entries appended but not yet persisted by the application (etcd's
/// `nextUnstableEnts`): the unstable tail not already being written.
fn RawNode::next_unstable(self : RawNode) -> Array[Entry] {
  self.log.next_unstable_ents()
}

///|
/// The committed-but-unapplied entries ready to hand to the state machine
/// (etcd's `nextCommittedEnts`). In the synchronous contract the caller persists
/// `entries` before applying these, so committed entries need not be capped at
/// the last stable index (etcd's `applyUnstableEntries` == `!asyncStorageWrites`):
/// a freshly committed entry surfaces as both `entries` and `committed_entries` in
/// the same `Ready`. Under async writes application waits for the append ack, so
/// they are capped at the stable index.
fn RawNode::next_committed(self : RawNode) -> Array[Entry] {
  self.log.next_committed_ents(!self.async_storage)
}

///|
/// Assemble a `Ready` without committing to handle it (etcd's
/// `readyWithoutAccept`): a pure read that leaves the buffered messages, read
/// states, and cursors untouched, so a caller may inspect pending work and
/// decide not to consume it.
pub fn RawNode::ready_without_accept(self : RawNode) -> Ready {
  self.sync_log()
  let node = self.raft.node()
  let entries = self.next_unstable()
  let committed = self.next_committed()
  let soft = self.raft.soft_state()
  let hard = node.hard_state()
  let hard_changed = hard != self.prev_hard
  let mut storage_append : StorageAppend? = None
  let mut storage_apply : StorageApply? = None
  if self.async_storage {
    // Hand the append (entries + any hard-state change) to the local append
    // thread. The response attests the current last (index, term) — not the last
    // of `entries` — so that a response arriving after a term change is dropped
    // (see StorageAppendResp / newStorageAppendResp).
    if entries.length() > 0 || hard_changed {
      let resp : StorageAppendResp = if self.log.has_next_or_in_progress_unstable_ents() {
        let last = self.log.last_entry_id()
        { index: last.index, log_term: last.term, term: node.current_term() }
      } else {
        { index: 0, log_term: 0, term: node.current_term() }
      }
      storage_append = Some({
        entries,
        hard_state: if hard_changed {
          Some(hard)
        } else {
          None
        },
        snapshot: None,
        resp,
      })
    }
    if committed.length() > 0 {
      storage_apply = Some({ entries: committed, resp: { entries: committed } })
    }
  }
  {
    soft_state: if soft == self.prev_soft {
      None
    } else {
      Some(soft)
    },
    hard_state: if hard_changed {
      Some(hard)
    } else {
      None
    },
    read_states: self.read_states.copy(),
    entries,
    snapshot: None,
    committed_entries: committed,
    messages: self.msgs.copy(),
    must_sync: must_sync(hard, self.prev_hard, entries.length()),
    storage_append,
    storage_apply,
  }
}

///|
/// Record that the caller has taken the given `Ready` and will handle it
/// (etcd's `acceptReady`): adopt the reported soft/hard states as the new
/// baseline, drop the served read states, and clear the drained message buffer.
/// The stable/applied watermarks move in `advance`, not here.
fn RawNode::accept_ready(self : RawNode, rd : Ready) -> Unit {
  if rd.soft_state is Some(s) {
    self.prev_soft = s
  }
  if rd.hard_state is Some(h) {
    self.prev_hard = h
  }
  // Mark the reported entries and committed entries as being written / applied,
  // so the next Ready does not re-offer them. `accept_unstable` is called
  // unconditionally, as in etcd's `acceptReady` (a no-op when the unstable tail is
  // empty or already in progress).
  self.log.accept_unstable()
  let c = rd.committed_entries.length()
  if c > 0 {
    self.log.accept_applying(
      rd.committed_entries[c - 1].index,
      ents_size(rd.committed_entries[:]),
      !self.async_storage,
    )
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
  self.sync_log()
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
/// Persist a `Ready`'s entries to stable storage (etcd's `storage.Append(rd.
/// Entries)`, which the caller runs *before* applying `committed_entries`). This
/// is the first half of handling a synchronous `Ready`: the caller `store`s, then
/// applies `committed_entries`, then calls `advance`. A no-op under async writes,
/// where the append is instead driven by `StorageAppend` + `step_append_resp`.
pub fn RawNode::store(self : RawNode, rd : Ready) -> Unit {
  if self.async_storage {
    return
  }
  if rd.entries.length() != 0 {
    self.log.commit_stable(rd.entries)
  }
}

///|
/// Notify the driver that the last `Ready` has been handled (etcd's `Advance`):
/// its entries were persisted (by `store`) and its committed entries applied, so
/// move the applied cursor past them. The stable watermark already moved in
/// `store`. A no-op under async writes (the responses drive the cursors).
pub fn RawNode::advance(self : RawNode, rd : Ready) -> Unit {
  if self.async_storage {
    return
  }
  let c = rd.committed_entries.length()
  if c != 0 {
    let last = rd.committed_entries[c - 1].index
    self.log.applied_to(last, ents_size(rd.committed_entries[:]))
    // Applying the committed entries releases their uncommitted-tail quota
    // (etcd releases at apply time, not commit time); this also advances the
    // core's applied watermark.
    self.raft.advance_applied(last)
  }
}

///|
/// Return a `StorageAppend` acknowledgement: the entries have been made durable.
/// The confirmed unstable prefix moves into stable storage and the unstable tail
/// is truncated — but only if the response is not stale: a response whose term is
/// below the node's current term (a later term has taken over) is ignored, and
/// the ABA guard in `async_stabilize` further requires the unstable log to still
/// hold that `(index, log_term)`.
pub fn RawNode::step_append_resp(
  self : RawNode,
  resp : StorageAppendResp,
) -> Unit {
  if resp.term < self.raft.node().current_term() {
    return
  }
  self.log.async_stabilize(resp.index, resp.log_term)
}

///|
/// Return a `StorageApply` acknowledgement: the committed entries have been
/// applied. Advance the applied cursor and release the applied entries' quota.
/// Committed entries are term-independent, so there is no staleness check.
pub fn RawNode::step_apply_resp(
  self : RawNode,
  resp : StorageApplyResp,
) -> Unit {
  let n = resp.entries.length()
  if n == 0 {
    return
  }
  let last = resp.entries[n - 1].index
  self.log.applied_to(last, ents_size(resp.entries[:]))
  self.raft.advance_applied(last)
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
    // The full synchronous cycle in one place: persist, (implicitly apply), then
    // advance. A caller with its own state machine runs the same three steps.
    self.store(rd)
    self.advance(rd)
  }
  out
}
