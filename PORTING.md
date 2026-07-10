# etcd-io/raft → MoonBit test-porting ledger

Source of truth: `etcd-io/raft` (Apache-2.0), cloned at `~/port/etcd-raft`.
This ledger tracks every `*_test.go` in the upstream repo, function by function
(and, for table- and data-driven tests, case by case). The discipline is:
**the upstream test is the source of truth**; where our implementation disagreed
we fixed the implementation, never the assertion.

Status legend:
- `DONE` — ported and green under `moon test`.
- `IMPL` — blocked on a missing feature that is being implemented first, then ported.
- `TODO` — portable onto the current API, not yet ported.
- `N/A` — cannot be ported meaningfully; reason given inline (Go-runtime / pure
  Go-language semantics / benchmark). Every `N/A` states an equivalent we do
  cover, if any.

Counts are updated per batch. `moon test` total after each batch is recorded at
the bottom.

---

## tracker/inflights_test.go — DONE (4/4)
Feature added: `inflights.mbt` (`Inflights` sliding-window flow control).
Ported in `inflights_wbtest.mbt`.
- `TestInflightsAdd` — DONE (both no-rotate and rotate sub-cases).
- `TestInflightFreeTo` — DONE (all 5 `FreeLE` steps incl. rotation).
- `TestInflightsFull` — DONE (all 7 table cases). Go's final `Add` panic is
  asserted as `full()==true` (its exact precondition); MoonBit `abort` is not
  catchable, so the panic itself cannot be trapped — behaviour is unchanged.
- `TestInflightsReset` — DONE (100-epoch reset loop).

## quorum/datadriven_test.go — DONE (127/127 cases)
Feature added: `quorum.mbt` (`committed_index`, `vote_result`, `VoteState`).
Ported in `quorum_wbtest.mbt`, every datadriven case expanded explicitly:
- `majority_commit.txt` — 16 cases DONE.
- `joint_commit.txt` — 50 cases DONE.
- `majority_vote.txt` — 27 cases DONE.
- `joint_vote.txt` — 34 cases DONE.

## quorum/majority_test.go — DONE (1/1)
- `TestDescribe` — DONE. Added `describe(voters, acked)` (etcd's
  `MajorityConfig.Describe` ASCII bar chart, exact format) in `quorum.mbt`; all 4
  cases in `quorum_describe_wbtest.mbt`.

## quorum/quick_test.go — DONE (1/1)
- `TestQuick` — DONE. `quorum_quick_wbtest.mbt` checks `committed_index` (simple
  and joint) against an O(n·max) brute-force reference over 3000 seeded random
  configurations — no discrepancies.

## quorum/bench_test.go — N/A (benchmark)
- `BenchmarkMajorityConfig_CommittedIndex` — N/A (performance benchmark, not a
  correctness test). `committed_index` correctness is covered by the 66 commit
  cases above.

---

## raft_paper_test.go — DONE (24/26)
Raft-paper property tests, ported onto `RaftNode`/`Node` across `paper_wbtest.mbt`
(18 cases), `randomized_timeout_wbtest.mbt` (4 election-timeout cases), and
`raft_core_port_wbtest.mbt` (6 cases) — 24 of 26 (pre-vote disabled to match
etcd's `newTestRaft` default):
- `TestFollowerUpdateTermFromMessage`, `TestCandidateUpdateTermFromMessage`,
  `TestLeaderUpdateTermFromMessage` — DONE.
- `TestStartAsFollower` — DONE.
- `testNonleaderStartElection` (follower) — DONE.
- `TestLeaderElectionInOneRoundRPC` — DONE (all 12 table cases).
- `TestFollowerVote` — DONE (6 cases).
- `TestCandidateFallback` — DONE.
- `TestLeaderStartReplication` — DONE.
- `TestLeaderCommitEntry` — DONE.
- `TestLeaderAcknowledgeCommit` — DONE (9 cases).
- `TestFollowerCommitEntry` — DONE.
- `TestFollowerCheckMsgApp` — DONE (5 cases; asserts the findConflictByTerm
  reject hint + term).
- `TestFollowerAppendEntries` — DONE (4 cases).
- `TestVoteRequest` — DONE (2 cases).
- `TestVoter` — DONE (9 cases).
- `TestLeaderOnlyCommitsLogFromCurrentTerm` — DONE (Figure 8, 3 cases).
- `TestLeaderSyncFollowerLog` — DONE (Figure 7, 6 cases; exercises the two-sided
  conflict backoff and full log overwrite).
- `TestCandidateStartNewElection`, `TestLeaderBcastBeat`,
  `TestLeaderCommitPrecedingEntries` — DONE (`raft_core_port_wbtest.mbt`).
- `TestFollower/CandidateElectionTimeoutRandomized`,
  `TestFollowers/CandidatesElectionTimeoutNonconflict` — DONE
  (`randomized_timeout_wbtest.mbt`). This required an implementation fix, not an
  interval-downgrade: every state transition now re-randomizes the election
  timeout (etcd's `reset()` — added to `become_leader`/`become_follower`/
  `become_candidate`), and the timeout spread is now drawn from the LCG's *high*
  bits — its low bits are non-random, so `% et` on the raw value never reached
  some values. Only `TestRejectStaleTermMessage` remains, covered by
  `b1_stale_term_wbtest.mbt` (24/26).

### findConflictByTerm (two-sided) — implemented
`Node::find_conflict_by_term` + `conflict_term` on `AppendEntriesReply` +
`Progress::maybe_decr_to`. The follower now hints `(index, term)` and the leader
refines it against its own log, matching etcd's `raft_test.go:TestFastLogRejection`
mechanism (that test itself is pending a full 2-node harness). Our own
`append_hint_wbtest.mbt` was corrected from the old one-step-backoff values to
the etcd findConflictByTerm values.

## Subsystem added: `unstable.mbt` + `raftlog.mbt` (etcd's `unstable` / `raftLog`)
A full `Unstable` (three-part `snapshot` / `offset` / `entries` + `offsetInProgress`
/ `snapshotInProgress` in-progress bookkeeping; snapshot as an `Option`, not a
sentinel) and `RaftLog` (`storage` + `unstable` + `committed` / `applying` /
`applied` + byte-level `max_applying_ents_size` pagination) were added.
**Wired (not orphan):** `RawNode` (`rawnode.mbt`) now keeps its unstable-vs-stable
split in a real `RaftLog` — the C-path `stabled` watermark is gone. `Ready.entries`
= `next_unstable_ents`, `Ready.committed_entries` = `next_committed_ents(false)`;
`advance` calls `commit_stable` (append to storage + `stable_to`) and `applied_to`.
Non-test proof: `run_single_node` (`rawnode_driver.mbt`) → `RawNode::new` →
`RaftLog::new_with_size`; grep `self.log.` in `rawnode.mbt`.

## log_test.go — DONE (21/21 ported against `RaftLog`)
Ported in `raftlog_wbtest.mbt` against the real `RaftLog`, every table case. The
`wpanic` cases assert a Go panic; MoonBit `abort` is not catchable (as with
inflights), so each such single case is noted and omitted while the rest run.
- `TestFindConflict` (10), `TestFindConflictByTerm` (21), `TestIsUpToDate` (9),
  `TestAppend` (4, asserts `unstable.offset`), `TestMatchTerm` (via
  `TestCompactionSideEffects`), `TestTerm` (5, real `ErrCompacted`/`ErrUnavailable`),
  `TestTermWithUnstableSnapshot` (5), `TestSlice` (~40, all size-cap thresholds),
  `TestScan` (scan≡slice sweep + early-break + max-out) — all DONE.
- `TestLogMaybeAppend` — 14/15 (the committed-conflict `wpanic` case aborts).
- `TestCompactionSideEffects`, `TestNextUnstableEnts`, `TestLogRestore`,
  `TestStableTo`, `TestStableToWithSnap` — DONE.
- `TestHasNextCommittedEnts` (14), `TestNextCommittedEnts` (14),
  `TestAcceptApplying` (18), `TestAppliedTo` (8) — DONE (apply-pacing).
- `TestCommitTo` — 2/3 (commit-past-last `wpanic` aborts).
- `TestCompaction` — DONE (the ErrCompacted lower-bound path; the out-of-upper-
  bound `wpanic` aborts).
- `TestIsOutOfBounds` — DONE (the ErrCompacted + in-range checks; two `wpanic`
  hi-out-of-bound cases abort).

## log_unstable_test.go — DONE (9/9 against `Unstable`)
Ported in `unstable_wbtest.mbt`, every table case:
`TestUnstableMaybeFirstIndex`, `TestMaybeLastIndex`, `TestUnstableMaybeTerm`,
`TestUnstableRestore`, `TestUnstableNextEntries`, `TestUnstableNextSnapshot`,
`TestUnstableAcceptInProgress` (15), `TestUnstableStableTo` (13),
`TestUnstableTruncateAndAppend` (9).

## Byte-level pagination — DONE (4/5; 1 N/A: async storage)
Commit pagination in `pagination_wbtest.mbt` (RawNode wired to
`max_applying_ents_size` via `raw_with_max_committed_size`); append pagination and
bounded log growth in `append_pagination_wbtest.mbt`. Once A-path wired
`MaxSizePerMsg` (into `send_to`, over the `limit_size`/`entry_encoding_size` this
layer provides) and `MaxUncommittedEntriesSize` (`uncommitted_size` tally), the
two previously-N/A tests became portable at the log/rawnode seam.
- `TestCommitPagination` — DONE. Three 1000-byte proposals commit in two batches
  (2 then 1) under a 2048-byte `MaxCommittedSizePerReady`.
- `TestRawNodeCommitPaginationAfterRestart` — DONE (simplified). 11 committed
  entries restart under a small cap and all apply in order, no gaps, none dropped;
  commit is set on restart rather than nudged by a `MsgHeartbeat` (a
  replication-core concern).
- `TestAppendPagination` — DONE. A leader with five ~1000-byte entries catches up
  a lagging follower; every `send_to` batch is asserted `<=` the 2048-byte cap
  (or a lone entry) and at least one batch exceeds half the cap — the byte-bounded
  batching, driven directly rather than through the network harness.
- `TestRawNodeBoundedLogGrowthWithPartitionedLeader` — DONE (etcd single-voter
  form). The A-path deviation B flagged is **fixed**: the uncommitted-tail quota
  is now released at **apply** time (`RaftNode::advance_applied`, called from
  `RawNode::advance`), matching etcd's `MsgStorageApplyResp` handler, not at
  commit time. So a single-voter leader's own appends commit immediately but the
  tally stays at `MaxUncommittedEntriesSize` (16 × 8 B) — bounding the tail and
  dropping further proposals — until Ready/Advance applies them, at which point
  the bytes release. Reproduces the etcd test verbatim.
- `TestCommitPaginationWithAsyncStorageWrites` — **DONE**. `AsyncStorageWrites` is
  now really implemented (typed storage directives on `Ready` + `step_*_resp`
  acks); see the "AsyncStorageWrites — DONE" section. `async_wbtest.mbt` drives the
  paginated apply batches through the async path.

## tracker/progress_test.go — DONE (8/8)
Ported in `progress_port_wbtest.mbt`. `Progress` now carries a real
`msg_app_flow_paused` throttle, a `pending_snapshot` index, an `is_learner` flag
and a `to_string` renderer, so every case ports faithfully (no downgrade):
- `TestProgressMaybeDecr` (10), `TestProgressUpdate` (4),
  `TestProgressBecomeReplicate`, `TestProgressBecomeSnapshot`,
  `TestProgressIsPaused` — DONE.
- `TestProgressString` — DONE (`Progress::to_string`, exact etcd format).
- `TestProgressResume` — DONE (`maybe_update`/`maybe_decr_to` clear the throttle).
- `TestProgressBecomeProbe` — DONE (3 cases; `become_probe` resumes from
  `max(match+1, pending_snapshot+1)` out of `Snapshot`).

## raft_flow_control_test.go — DONE (3/3)
`Inflights` is wired into `Progress` (`is_paused`/`sent_entries`/`free_le`),
`send_to`/`bcast_append` (throttle a full streaming window) and
`handle_append_resp` (release acked slots). Ported in `flow_control_wbtest.mbt`:
- `TestMsgAppFlowControlFull` — DONE.
- `TestMsgAppFlowControlMoveForward` — DONE.
- `TestMsgAppFlowControlRecvHeartbeat` — DONE (see MsgHeartbeatResp below).

## AsyncStorageWrites — DONE (real implementation, not equivalence)
Previously registered N/A with an equivalence proof; now **implemented for real**
in `ready.mbt` / `rawnode.mbt` / `raftlog.mbt`. etcd puts the four directives
(`MsgStorageAppend`/`Resp`, `MsgStorageApply`/`Resp`) in `Ready.Messages` because
it has a single uniform `Message` type; here they are typed local directives on
the `Ready` — `storage_append : StorageAppend?` and `storage_apply :
StorageApply?` (each carries the entries plus the response the caller returns).
This keeps everything inside the log/rawnode boundary and off the `message.mbt`
`Payload` enum (which A-path is concurrently extending), while modelling exactly
what etcd's local `To=LocalAppendThread`/`LocalApplyThread` messages are: I/O
directives, not network messages.

- **`RaftNode::raw_async` / `raw_async_sized`** enable the mode. When on,
  `ready_without_accept` builds a `StorageAppend` (entries + any HardState change,
  with a `StorageAppendResp` attesting the current last `(index, term)` and the
  node's term) and a `StorageApply` (committed entries + resp), and `advance`
  becomes a no-op.
- **`RawNode::step_append_resp` / `step_apply_resp`** are the acks. Append →
  `RaftLog::async_stabilize` (move the confirmed unstable prefix into storage +
  truncate the unstable tail). Apply → `applied_to` + `advance_applied`.
- **Non-orphan wiring:** the directives are produced by `ready_without_accept` and
  consumed by the two step methods — both non-test code; `raw_async` off is
  byte-for-byte the synchronous path (all pre-existing tests unchanged).
- **ABA safety (the important part).** A late `StorageAppendResp` must not confirm
  a log a newer term has since overwritten. Two guards, both tested:
  1. `step_append_resp` drops any ack whose `term` is below the node's current
     term (a later term has taken over) — this is the essential guard, because
     re-replicated entries keep their original term, so at a given index the term
     can read X → Y → X again while the *current* term only climbs.
  2. `async_stabilize` additionally requires the unstable log to *still* hold
     `(index, log_term)`; a mismatch (index rewritten) is a no-op.

Ported (`async_wbtest.mbt`):
- Single-node no-op committed through `StorageAppend`/`StorageApply` directives.
- `async_storage_writes_append_aba_race.txt` — DONE, both the term-changed race
  (a term-1 ack arrives after a term-2 rewrite) and the subtle term-cycled race
  (a term-1 ack whose `(index, term)` matches the *re-appeared* term-1 entries but
  whose send-term is below the current term-3). The stale ack stabilizes nothing;
  the current ack confirms the correct entries.
- `TestCommitPaginationWithAsyncStorageWrites` — DONE. Under async writes *and* a
  per-Ready commit byte cap, three proposals apply in order across paginated
  `StorageApply` batches (no batch over the cap, at least one split), none dropped.
- `async_storage_writes.txt` — the straight-line (non-ABA) append/apply pipeline is
  covered by the single-node and pagination tests above; the multi-node network
  orchestration in that script is the sim/interaction layer (mapped there).

Design note for the coordinator: I modelled the storage directives as typed
`Ready` fields + `step_*_resp` methods rather than four new `Payload` enum variants
in `message.mbt`, to (a) avoid the flagged enum collision with A-path's concurrent
`Propose`/`ReadIndex`/… additions, and (b) reflect that these are *local* I/O
directives, not transportable messages. If you'd rather have the `Payload`-variant
representation for uniformity, the exact variants are `MsgStorageAppend{entries,
hard_state?, snapshot?}` / `MsgStorageAppendResp{index, log_term, term}` /
`MsgStorageApply{entries}` / `MsgStorageApplyResp{entries}`, and I'll re-wire onto
them once they exist.

## MaxSizePerMsg + MaxUncommittedEntriesSize — DONE (consensus-core)
Both size limits the B-path marked N/A (they live in the consensus core, not the
log layer) are now implemented in `raftnode.mbt`, calling B's `limit_size` /
`payload_size`:
- **MaxSizePerMsg** — `send_to` caps each outbound AppendEntries batch by encoded
  byte size (`limit_size`), keeping at least one entry. Test in
  `raft_limits_wbtest.mbt`.
- **MaxUncommittedEntriesSize** — `RaftNode` tracks `uncommitted_size`;
  `propose`/`propose_conf` drop a proposal that would push the uncommitted tail
  over the cap (unless the tail is empty, or the entry is zero-byte);
  `reduce_uncommitted_size` releases bytes as entries commit (auto on commit, and
  exposed for the app). Ports `TestUncommittedEntryLimit` faithfully (1024
  accepted, 1025th dropped, single-large accepted, empty always accepted).

## B5 — committed-entry truncation guard — DONE
`store_entries` now aborts if an incoming entry would conflict at or below the
commit index (which would delete a committed entry, breaking State-Machine
Safety). Unreachable for a correct leader given the log-matching and stale-term
(B1) guards; the assertion turns any regression into a loud failure.

## ReadOnly (§6.4) — DONE (both modes)
`read_only.mbt` adds a `ReadOnly` tracker with both etcd modes.
`request_read_index(ctx)`: in `ReadOnlySafe` it records the read at the current
commit index and broadcasts heartbeats stamped with `ctx`; a follower echoes the
context; `handle_heartbeat_resp` feeds `recv_ack`, and once a quorum has acked a
context every read up to it (FIFO) is confirmed. In `ReadOnlyLeaseBased` (the
default) a valid lease confirms the read at once; a single-node leader always
does. Confirmed reads are drained via `take_read_states()` (reusing C's
`ReadState`). Precondition enforced: the commit index must be from the current
term (§5.4.2). Tests in `read_only_wbtest.mbt` cover Safe (quorum round-trip,
FIFO batch confirm), LeaseBased, follower-serves-nothing, and single-node.
Covers the core of etcd `TestReadOnlyOptionSafe` / `TestReadOnlyOptionLease` /
`TestReadOnlyForNewLeader`.

## MsgHeartbeat / MsgHeartbeatResp — DONE
Heartbeats are now a distinct message type (`Heartbeat`/`HeartbeatResp`), not an
empty AppendEntries. `bcast_heartbeat` emits a `Heartbeat` carrying the leader's
commit index (capped at the follower's match); `handle_heartbeat` advances the
follower's commit and applies committed conf changes; `handle_heartbeat_resp` is
pure liveness (marks active for check-quorum/lease) and resumes replication —
sending the entries a behind follower lacks, or a content-free probe when its
in-flight window is full. An empty AppendEntries is now labelled `Probe`.

## raft_snap_test.go — DONE (3/5)
Added Snapshot-state recovery: a successful `AppendResp` at/past the snapshot
baseline aborts the pending snapshot and resumes streaming; `bcast_append` now
skips a paused (snapshot-pending or window-full) follower. Ported in
`raft_snap_wbtest.mbt`:
- `TestSnapshotAbort` — DONE.
- `TestPendingSnapshotPauseReplication` — DONE.
- `TestSendingSnapshotSetPendingSnapshot` — DONE (real `pending_snapshot`).
- `TestSnapshotFailure` / `TestSnapshotSucceed` — DONE. Added
  `RaftNode::report_snapshot(id, reject)` (etcd's ReportSnapshot / MsgSnapStatus)
  and `report_unreachable(id)` (ReportUnreachable): a report leaves the Snapshot
  state, discards the pending snapshot on failure, and pauses until the next
  AppendEntries response. **5/5.**

## B3 — check-quorum leader stickiness — DONE (core)
With check-quorum on, a server still in contact with a leader (`leader_id` set,
election timer not expired) now rejects both vote and pre-vote solicitations, and
a higher-term vote is refused *without* adopting the challenger's term — so a
partitioned node cannot disrupt a stable leader or ratchet the term (§4.2.3).
Implemented as `in_lease` + a guard in `step`'s Ahead branch and in the vote
handlers. Tests in `checkquorum_wbtest.mbt` (reject vote, reject pre-vote, grant
again once the lease lapses). Covers the core of etcd
`TestLeaderSupersedingWithCheckQuorum` / `TestDisruptiveFollower`.

## leadTransferee state machine (§3.10) — DONE (core)
`RaftNode` now tracks `lead_transferee`: `transfer_leadership` validates the
target (no self, no non-member/learner), records it, and either sends
`TimeoutNow` (target caught up) or first replicates; while a transfer is in
flight proposals are refused; the target is handed off the moment its ack
catches it up to the last index; the transfer aborts on an election-timeout, on
step-down, and when the target is removed. Tests in `leader_transfer_wbtest.mbt`.
Covers the core of etcd `TestLeaderTransferToUpToDateNode` /
`ToSlowFollower` / `Timeout` / `ToSelf` / `ToNonExistingNode` /
`IgnoreProposal` / `RemoveNode`.

The transfer *request* is also a routable message now (etcd's MsgTransferLeader),
not only its result (`TimeoutNow`): `TransferLeader(target)` carries the intended
new leader. On a leader it begins the handoff; on a follower it is forwarded to
the leader, so a transfer may be initiated from any server
(`request_transfer_leader`, tested in `message_routing_wbtest.mbt`).

## raft_test.go — DONE (partial) + IMPL
Ported onto a single RaftNode in `raft_core_port_wbtest.mbt` (plus B2/B3/
leadTransferee tests above): `TestSingleNodeCommit`, `TestVoteFromAnyState`,
`TestPreVoteFromAnyState`, `TestAddNode`/`TestRemoveNode`/`TestCommitAfterRemoveNode`
(via `confchange_apply_wbtest.mbt`), `TestLearnerPromotion`/`TestLearnerElectionTimeout`
(via `learner_wbtest.mbt`), the check-quorum superseding/disruptive family, and
the leader-transfer family. Remaining (network-harness or another workstream):
ReadOnlySafe two-mode, dueling-candidates / overwrite-newer-logs, RawNode/Ready.

## raft_paper_test.go additional
`TestCandidateStartNewElection` and `TestLeaderCommitPrecedingEntries` now ported
in `raft_core_port_wbtest.mbt` (20/26 total).

## rawnode_test.go — DONE (9/12 ported, 3 N/A; +2 benchmarks N/A)
Feature added: `rawnode.mbt` (`RawNode`), `ready.mbt` (`Ready`/`SoftState`/
`ReadState`/`HardState` change-detection + `must_sync`), `rawnode_driver.mbt`
(`run_single_node`). `RawNode` wraps the existing `RaftNode`: the core's
message-returning `tick`/`step`/`propose`/`campaign` are batched into the
synchronous Ready/Advance cycle. A `stabled` watermark on `RawNode` splits
entries-to-persist (`entries`) from committed-to-apply (`committed_entries` =
`(applied, min(commit, stabled)]`); capping commit at `stabled` reproduces
etcd's "persist before apply" sequencing (an entry surfaces as `entries` in one
Ready, `committed_entries` in the next). Ported in `rawnode_port_wbtest.mbt`.
- `TestRawNodeStep` — DONE (adapted). etcd rejects *local* message types at
  runtime; in our type system a local op cannot be built as a `Message` (they
  are the distinct methods `campaign`/`tick`/`propose`/`read_index`), so
  rejection is a compile-time guarantee. Test walks all 9 network `Payload`
  variants through `Step`.
- `TestRawNodeProposeAndConfChange` — DONE (V1 `AddNode` + 6/7 V2 cases). Ported
  in `confchange_rawnode_wbtest.mbt` over `RawNode::propose_conf_v2` /
  `conf_state` (new, delegating to the A-path engine), asserting the resulting
  `ConfState` sets: V2 add-learner settles simple (case 3); explicit joint learner
  + manual leave (case 4, exp and exp2); implicit joint learner auto-leaves (case
  5); add-and-demote uses `LearnersNext` implicit/explicit (cases 6–8, exact
  `{voters 2, outgoing 1, learners 3, learners_next 1}`). **Residual gap**: the
  V2 simple-single-add case (case 2, `Voters{1,2}` with no outgoing) needs etcd's
  `Transition=Auto` simple-apply, but this port's `ConfChangeV2` has only
  `auto_leave` (implicit/explicit joint) and its apply always enters joint for a
  non-empty batch — a single voter-add cannot be applied simply. Flagged to the
  coordinator (would need a `Transition`/simple-apply on the A-path engine). Also
  the `ConfState.auto_leave` flag is transient here (cleared when the leave entry
  is appended, not when it commits), so the set-fields are asserted exactly and
  the flag noted rather than compared.
- `TestRawNodeJointAutoLeave` — DONE (behavioural). An implicit-joint learner add
  enters C(old,new) and auto-leaves to `{voters 1, learners 2}`. etcd parks the
  config in the joint state by stepping the leader down and re-electing before the
  auto-leave fires; this port applies committed conf changes in the core and
  leaves as soon as the joint quorum allows, so the settled end state is asserted
  (the enter-joint/auto-leave lifecycle is also covered end to end by
  `joint_wbtest.mbt`).
- `TestRawNodeProposeAddDuplicateNode` — DONE (V1). Duplicate `AddNode` still
  appends its entry, so the log holds cc1, cc1, cc2.
- `TestRawNodeReadIndex` — DONE (both halves): a recorded `ReadState` is
  surfaced by `Ready` and reset on accept; `read_index` on a leader records a
  `ReadState` at the commit index carrying the caller's context. etcd's second
  half installs a `step` hook to observe a `MsgReadIndex`; we assert the
  observable outcome (the `ReadState`) since read entry-points are methods.
- `TestRawNodeStart` — DONE (adapted). etcd bootstraps from a `MemoryStorage`
  snapshot at index 1 (`FirstIndex >= 2`); we start a fresh node, so indices are
  one lower and the two committed entries surface across two Readys (our
  single-node commits the election no-op eagerly). The lifecycle invariants are
  identical and all checked: entry emitted for persistence before application,
  `MustSync` set only while a durable write is pending, `HasReady` → false at
  quiescence.
- `TestRawNodeRestart` — DONE. Recovered committed prefix emits only
  `committed_entries`, no HardState, `MustSync` false.
- `TestRawNodeRestartFromSnapshot` — DONE (snapshot at index 2 + one entry).
- `TestRawNodeStatus` — DONE (both halves). Leader/role/term as before; the
  progress-map and config half is now ported in `confchange_rawnode_wbtest.mbt`
  over `RawNode::full_status` / `progress_of`: after a 3-voter election the
  leader's own `ProgressStatus` is present (match ≥ 1, not a learner), progress is
  tracked for all three members (etcd's `status.Progress[1]`), and `full_status`
  reports the three voters as the configuration (etcd's `tracker.Config`).
- `TestRawNodeCommitPaginationAfterRestart` — **DONE** (now that RawNode carries a
  real per-Ready byte budget). See the "Byte-level pagination" section.
- `TestRawNodeBoundedLogGrowthWithPartitionedLeader` — **DONE** (A-path added the
  `uncommitted_size` backpressure counter). See the "Byte-level pagination"
  section.
- `TestRawNodeConsumeReady` — DONE. `ready_without_accept` leaves the message
  buffer; `ready` drains it; `advance` does not drop a message enqueued after.
- `BenchmarkStatus`, `BenchmarkRawNode` — **N/A** (benchmarks).

## node_test.go — DONE (12/22 ported, 10 N/A)
etcd's `Node` is a goroutine loop over Go channels; `RawNode` is its documented
goroutine-free equivalent, so protocol-level tests map directly. Ported in
`node_port_wbtest.mbt`. Tests that assert channel/goroutine/context plumbing
(which MoonBit has no analogue for — no channels, no goroutines) are N/A.
- `TestSoftStateEqual` — DONE (3 cases).
- `TestIsHardStateEqual` — DONE (4 cases).
- `TestNodeStep` — DONE (adapted): proposals route through `Propose`, network
  messages through `Step`; a stepped network message yields a buffered reply.
- `TestNodePropose` — DONE (leader append surfaced by Ready).
- `TestNodeProposeConfig` — DONE (V1 conf-change appended, payload preserved).
- `TestNodeProposeAddDuplicateNode` — DONE (mapped to RawNode; 4 committed
  entries: no-op + cc1 + cc1 + cc2).
- `TestBlockProposal` — DONE (adapted): a pre-leader proposal produces no entry;
  a post-leader one is appended. (etcd blocks the caller goroutine; the
  observable equivalent without goroutines is the dropped proposal.)
- `TestNodeTick` — DONE (one tick advances the election clock by one).
- `TestNodeAdvance` — DONE (a committed entry is available to apply after
  Advance).
- `TestNodeStart` — DONE (adapted): HardState progression (vote+commit on
  election, commit bump on proposal, apply-only final Ready with `MustSync`
  false). The `StartNode` faux-ConfChange bootstrap is not modeled (V1 boundary).
- `TestNodeRestart` — DONE.
- `TestNodeRestartFromSnapshot` — DONE.
- `TestNodeStepUnblock` — **N/A**: `Step` blocking on an unbuffered channel,
  unblocked by `close(done)` or `context` cancel. No channels/goroutines.
- `TestDisableProposalForwarding` — **DONE** (`message_routing_wbtest.mbt`). A
  `Propose` (etcd's MsgProp) is now a routable message: a follower forwards it to
  the known leader (preserving the origin as `from`), and drops it when
  `disable_proposal_forwarding` is set. Verified both arms: the default follower
  emits one forwarded `Propose` to the leader; the disabled follower emits none.
- `TestNodeReadIndexToOldLeader` — **DONE** (`message_routing_wbtest.mbt`). A
  `ReadIndex` request forwards to the known leader with no term stamped and the
  origin preserved, so the answer routes back even across a leader change: the
  old leader (now a follower of the new one) re-forwards the held requests to the
  new leader, each still carrying its original origin.
- `TestNodeProposeWaitDropped` — **DONE** (non-goroutine part, in
  `message_routing_wbtest.mbt`): a dropped proposal produces no messages and does
  not touch the log. Ported as a candidate proposal (no leader to forward to) —
  the observable "proposal dropped" outcome. The goroutine + `context`-timeout +
  `step`-hook injection wrapper remains a Go-concurrency concern (N/A).
- `TestNodeStop` — **N/A**: goroutine lifecycle and `Stop` idempotency; the empty
  `Status{}` after stop has no analogue.
- `TestNodeProposeAddLearnerNode` — **DONE**. Ported in
  `confchange_rawnode_wbtest.mbt`: proposing an `AddLearnerNode` conf change makes
  node 2 a learner while the voter set stays `{1}`, and the committed entry
  decodes back to that learner add. (etcd drives this through the goroutine `Node`
  loop; the assertion — learner added, voters unchanged — is on the resulting
  `ConfState`, over the synchronous RawNode.)
- `TestAppendPagination` — **DONE** (the byte-budget assertion is driven at the
  `send_to` seam rather than the rafttest network harness). See the "Byte-level
  pagination" section.
- `TestCommitPagination` — **DONE** (RawNode now applies to a per-Ready byte
  budget). See the "Byte-level pagination" section.
- `TestCommitPaginationWithAsyncStorageWrites` — **DONE**: `AsyncStorageWrites` is
  now implemented; see the "AsyncStorageWrites — DONE" section.
- `TestNodeCommitPaginationAfterRestart` — **DONE** via its rawnode twin
  `TestRawNodeCommitPaginationAfterRestart`. See the "Byte-level pagination"
  section.

### asyncStorageWrites — DONE (implemented)
No longer deliberately skipped. `AsyncStorageWrites` is implemented as typed local
storage directives on `Ready` (`storage_append`/`storage_apply`) with
`step_append_resp`/`step_apply_resp` acks, including the ABA race protection. It
needs no language-level `async` — it is pure directive/response message passing,
exactly as etcd's local storage-thread messages are. See the top-level
"AsyncStorageWrites — DONE" section for the full write-up and tests
(`async_wbtest.mbt`).

## B2 — committed ConfChange applied to the live node — DONE (core)
Fixed the biggest gap: a committed `ConfChange` now mutates the running server,
not just a side `ConfDriver`. `RaftNode::apply_committed_conf` folds every
committed-but-unapplied conf entry (tracked by `conf_applied`) into the live
`config`, `progress` and `peers`; `maybe_commit` re-evaluates in a loop because a
removal can shrink the quorum and unblock further commits; a leader that removes
itself steps down. Both leader (`maybe_commit`) and follower (`handle_append`)
apply. End-to-end tests in `confchange_apply_wbtest.mbt`:
- AddNode makes the newcomer a voter with its own progress — DONE.
- Leader removing itself steps down and leaves the config — DONE.
- Removal shrinks the quorum and unblocks a pending commit — DONE.
The three items this section once listed as outstanding are now done and are
tracked in their own sections below: joint auto-leave (`joint_wbtest.mbt`),
learner demotion including the `learners_next` staging
(`learners_next_wbtest.mbt`), and aborting an in-flight leadership transfer to a
removed target (`leader_transfer_wbtest.mbt`).

## learners (§4.2.1) — DONE (core)
`Membership` gained a `learners` set (`add_learner`/`is_learner`/`nodes`); a
learner receives replication (peer + progress) but is never counted in
`committed_index`/`vote_result` and never campaigns (`campaign` promotable
guard). `ConfChange` gained `AddLearnerNode` ('L' tag); `AddNode` on a learner
promotes it. Wired through `apply_conf_change`/`ensure_peer`. End-to-end tests in
`learner_wbtest.mbt`:
- learner joins non-voting then is promoted — DONE.
- learner never campaigns — DONE.
- leader replicates to a learner — DONE.

## Joint consensus + ConfChangeV2 auto-leave (§4.3) — DONE (core)
`ConfChangeV2` (a batch of single changes + `auto_leave`) with its own log
encoding (`V` prefix). `propose_conf_v2` enters joint C(old,new): the current
voters become the outgoing half and the batch is folded into the incoming half
(`begin_joint`); commit then needs a majority of *both* halves (already handled
by `committed_index`/`vote_result`). With `auto_leave`, the leader appends the
matching LeaveJoint automatically once the EnterJoint applies; committing it
settles on the new configuration. `reconcile_peers` keeps progress/peers in line
across a change (add/drop, transfer-abort, leader self-removal step-down). Tests
in `joint_wbtest.mbt` (encoding round-trip, enter→auto-leave, both-majorities
safety). Covers the core of `TestRawNodeJointAutoLeave` and the confchange
EnterJoint/LeaveJoint path.
- **auto-leave is durable, not transient (bug fix).** `auto_leave` now lives in
  `Membership` (set from the committed EnterJoint, cleared only when the
  LeaveJoint commits/applies), *not* a marker cleared when the LeaveJoint is
  appended. Only committed entries change state (Raft §5.3): if the appended
  LeaveJoint is truncated by a new leader before it commits, the config is still
  an auto-leave joint, and the leader re-appends the LeaveJoint
  (`maybe_append_auto_leave` + `has_pending_leave` scan the uncommitted tail;
  also driven from `become_leader`). Red→green regression in
  `auto_leave_truncation_wbtest.mbt` (fails before the fix: `auto_leave` reads
  false once the LeaveJoint is merely appended). `ConfState.auto_leave` is now a
  reliable field to assert on.
- **`ConfChangeTransition` + simple-apply path.** `ConfChangeV2` now carries a
  `transition` (`Auto` / `JointImplicit` / `JointExplicit`, encoded in the log).
  `enters_joint()` implements etcd's `ConfChangeV2.EnterJoint` rule: an `Auto`
  batch changing at most one voter is applied **simply** (no joint, `Voters{1,2}`
  with empty outgoing), matching etcd — anything else enters joint, auto-leaving
  unless explicit. This unblocks case 2 of `TestRawNodeProposeAndConfChange`.
  Tests in `conf_transition_wbtest.mbt` (the `enters_joint` decision table, a
  live single-voter simple apply, an explicit-joint single change). The change is
  effectively additive: all `ConfChangeV2` construction goes through the
  `enter_joint`/`leave_joint`/`auto` constructors (`rawnode.mbt` only receives
  the value), verified by `moon check --deny-warn --target all`.

## confchange package (Changer + learners_next + Restore) — DONE
Implemented the confchange-package `Changer` (`changer.mbt`), separate from the
live `RaftNode` apply path: `ChangerConfig` (incoming/outgoing/learners/
`learners_next`/auto_leave) + a `ProgressMap`, with `simple` / `enter_joint` /
`leave_joint` / `restore`, transactional (rolled back on error, etcd's
`checkAndCopy`), progress anchored at `max(last_index, 1)`, and a `describe`
renderer in etcd's datadriven format.
- **`learners_next`** (the core invariant) — a voter demoted *during* a joint
  change is staged in `learners_next`, stays a voter via the outgoing half, and
  becomes a learner (progress preserved, not recreated) only on `leave_joint`.
  Ported `joint_learners_next.txt` exactly, incl. the `next`-round check.
- Also ported `simple_promote_demote.txt`, the simple-safety error cases (>1
  voter changed / simple-while-joint), and (bug fix) `add_learner`-demotes-voter
  in the live `Membership`.
- **`confchange/restore_test.go`** — DONE: `Changer::restore(ConfState)` runs the
  outgoing→incoming change sequence (etcd's `toConfChangeSingle`/`Restore`);
  ported the joint worked-example and a plain ConfState in `changer_wbtest.mbt`.
- **`ConfState` wired into the live `Snapshot`** — DONE. `Snapshot` and
  `InstallSnapshotArgs` now carry a `conf_state`; a leader stamps its live
  membership (`RaftNode::conf_state`) into every snapshot it ships, and a follower
  installing one rebuilds its voter / outgoing / learner sets
  (`restore_conf_state`, wired into `handle_snapshot`) instead of silently losing
  them. Tests in `snapshot_confstate_wbtest.mbt` (simple + joint restore + a
  leader→follower round-trip). Adding the field touched every `Snapshot` literal
  (source + tests) — a mechanical one-field compile-fix, no logic changed.
- **`learners_next` in the LIVE path** — DONE. The staging is no longer only in
  the offline `Changer`: `Membership` now carries `learners_next`, and
  `add_learner` on a voter that is still an outgoing voter stages it there (via
  `apply_conf_change_v2`, which now snapshots the outgoing half *before* applying
  the batch); `leave_joint` promotes staged nodes to real learners; `remove`/`add`
  clear the staging; `reconcile_peers` preserves (never recreates) their
  Progress; `conf_state`/`restore_conf_state` carry it through a snapshot. So a
  voter demoted mid-joint keeps voting via the outgoing half and only stops
  counting after leave — the production path now matches what the tests assert.
  End-to-end proof in `learners_next_wbtest.mbt`: node 4, demoted in joint,
  blocks a commit that needs the outgoing 3-of-4 until a third ack, then after
  leave becomes a non-voting learner with its Progress (its `next` round)
  intact; plus a snapshot-restore case.

## interaction_test.go — DONE (driveable corpus ported; rest mapped per-file)
`TestInteraction` is a `datadriven` string-snapshot harness: `InteractionEnv`
runs a script of commands over a cluster and diffs the *exact* rendered output.
Two things make a verbatim string port impossible here, independent of effort:
1. the output is dominated by the raft **core's logger** ("became candidate",
   "cast MsgVote for 1", "became leader", the `newRaft` banner) — that logging
   lives in the off-limits core (`raftnode.mbt`/`replication.mbt`) and this port
   does not emit it; and
2. ids render as `uint64` (and hex in `describeTarget`), whereas ids here are
   `String`s, so even the reproducible `Ready`/message lines cannot match
   character-for-character.
So each **driveable** script is ported *behaviourally* over the same public
RawNode Ready/Advance contract the harness uses — a minimal multi-node
`InteractionCluster` in `interaction_wbtest.mbt` (drives the Ready pipeline, which
`sim.mbt` does not: sim drives the raw `RaftNode.step`). Asserted on the `Ready`
contents and end state each script encodes, not the rendered strings.

Ported (`interaction_wbtest.mbt`):
- `single_node.txt` — DONE. Single voter campaigns, persists its no-op, then
  applies it (the Ready persist→apply split). Bootstrap-at-index-3 is a
  raftLog-restore detail covered by `TestLogRestore` /
  `TestRawNodeRestartFromSnapshot`.
- `campaign.txt` — DONE (behavioural). One candidate wins a 3-voter election; the
  no-op replicates to every log and commits cluster-wide (the trailing commit is
  propagated by a heartbeat, as etcd's trailing `MsgApp Commit:3` does).
- A propose→replicate→commit→apply scenario (the shape shared by
  `lagging_commit.txt` / `probe_and_replicate.txt`) — DONE: a proposal commits on
  every node and the leader applies it exactly once.

Mapped per file (behaviour covered elsewhere; the *exact datadriven string* is
N/A for reasons 1–2 above):
- `campaign_learner_must_vote.txt`, `confchange_disable_validation.txt`,
  `confchange_v1_add_single.txt`, `confchange_v1_remove_leader.txt`,
  `confchange_v1_remove_leader_stepdown.txt`, `confchange_v2_add_double_auto.txt`,
  `confchange_v2_add_double_implicit.txt`, `confchange_v2_add_single_auto.txt`,
  `confchange_v2_add_single_explicit.txt`, `confchange_v2_replace_leader.txt`,
  `confchange_v2_replace_leader_stepdown.txt` — **confchange/membership** (Simple /
  joint / learner / leader-replace). Off-limits (`confchange.mbt`/`changer.mbt`/
  `membership.mbt`); covered by the A-path `confchange/*` and B2/B3 ports
  (`confchange_apply_wbtest.mbt`, `joint_wbtest.mbt`).
- `prevote.txt`, `prevote_checkquorum.txt`, `checkquorum.txt`, `campaign.txt`
  (multi-term details) — **election / PreVote / CheckQuorum**. Core, off-limits;
  covered by the A-path election / `raft_paper_test` ports and `sim.mbt` election
  scenarios.
- `forget_leader.txt`, `forget_leader_prevote_checkquorum.txt`,
  `forget_leader_read_only_lease_based.txt` — **DONE** (expanded step-by-step in
  `message_routing_wbtest.mbt`). `ForgetLeader` (etcd's MsgForgetLeader) is now a
  routable message: a follower clears its recognised leader without moving its
  term or resetting its election timer (so it may grant (pre)votes at once after a
  partition); a candidate/leader is a no-op; and it is ignored under lease-based
  reads, where forgetting the leader would undermine the lease reads depend on.
  The three scripts' essence is covered: the base clear + candidate no-op + timer
  invariance; the prevote/check-quorum interaction (a leased follower refuses a
  pre-vote, then grants it once the leader is forgotten); and the lease-based
  ignore.
- `lagging_commit.txt`, `replicate_pause.txt`, `probe_and_replicate.txt`,
  `heartbeat_resp_recovers_from_probing.txt` — **replication flow control**
  (Probe/Replicate, inflights, heartbeat-resp recovery). Core (`replication.mbt`/
  `progress.mbt`), off-limits; covered by `flow_control_wbtest.mbt` and the
  behavioural replicate port above.
- `slow_follower_after_compaction.txt`, `snapshot_succeed_via_app_resp.txt`,
  `snapshot_succeed_via_app_resp_behind.txt` — **snapshot send/apply + storage
  compaction**. `snapshot.mbt`/`snapshot_rpc.mbt` off-limits; storage compaction
  is covered by `TestStorageCompact`/`TestCompaction`, snapshot behaviour by the
  A-path snapshot ports and `sim.mbt`.
- `async_storage_writes.txt`, `async_storage_writes_append_aba_race.txt` — **N/A
  (acceptable kind 1: Go threading-execution)**. `AsyncStorageWrites` with
  `MsgStorageAppend`/`MsgStorageApply`; the synchronous Ready/Advance path is the
  documented equivalent (see the AsyncStorageWrites section).

## storage_test.go — DONE (8/8)
Feature added: `storage_engine.mbt` rewritten to etcd's `Storage` contract with a
`StorageError` suberror ADT (`Compacted` / `Unavailable` / `SnapOutOfDate` /
`SnapshotTemporarilyUnavailable`) and `-> T raise StorageError` signatures; the
error variants are wired into non-test code in `storage_bridge.mbt`
(`save_into` catches `SnapOutOfDate`; `load_from` catches
`SnapshotTemporarilyUnavailable` and `Compacted`/`Unavailable`). Ported in
`storage_port_wbtest.mbt`, every table case:
- `TestStorageTerm` — DONE (5 cases; `Compacted` vs `Unavailable` now distinct).
- `TestStorageEntries` — DONE (11 cases incl. all size-cap thresholds via
  `entry_encoding_size`/`limit_size`).
- `TestStorageLastIndex` — DONE (2 cases).
- `TestStorageFirstIndex` — DONE (2 cases, incl. post-`compact`).
- `TestStorageCompact` — DONE (4 cases; `Compacted` at/below sentinel).
- `TestStorageCreateSnapshot` — DONE (2 cases, incl. ConfState). Fixed a bug: the
  port's `create_snapshot` used to build the snapshot with an empty ConfState,
  dropping the membership; it now takes a `conf_state?` param (etcd's `cs`) and
  records/retains it. The test supplies `ConfState{voters:[1,2,3]}` and asserts it
  round-trips (see GAP_logstore.md).
- `TestStorageAppend` — DONE (all 7 cases; exact post-append `ents` layout).
- `TestStorageApplySnapshot` — DONE (3 cases; normal / `SnapOutOfDate` /
  bootstrap-with-index-0). Semantic fix: `apply_snapshot` no longer bumps the
  commit index (etcd's `ApplySnapshot` leaves HardState untouched); the old
  behaviour diverged.

## Network-orchestration safety — DONE (core)
Ported the safety essence of etcd's `TestDuelingCandidates` /
`TestLeaderElectionOverwriteNewerLogs` over the deterministic cluster simulator
in `network_safety_wbtest.mbt`: a minority partition cannot elect a leader
(Election Safety under a split); a healed partition reconverges to one leader
and commits everywhere; an isolated leader is demoted by check-quorum and its
stale log never overwrites the majority's after rejoin (asserted via
`one_leader_per_term` / `committed_agrees` / `logs_consistent`).

## rafttest/{network,node}_test.go — N/A (Go network-harness) with named mapping
These exercise etcd's `rafttest` harness: a `raftNetwork` of Go channels with
per-connection drop/delay/pause goroutines (`network_test.go`), and a goroutine
per node running `Node.run()` off `select` over channels (`node_test.go`:
`TestBasicProgress`, `TestRestart`, `TestPause`). They test the *harness's*
concurrency plumbing (channel scheduling, goroutine pause/resume), not new
consensus properties — so they are N/A of the acceptable first kind (Go runtime
threading), and our deterministic `sim.mbt` provides the named equivalent:
- `raftNetwork` drop/delay/partition → `Cluster::set_drop` / `set_delay` /
  `partition` / `isolate` / `heal` (`sim.mbt`).
- `TestNetworkDrop` (a proposal replicates despite lossy links) →
  `chaos_wbtest.mbt` + `network_safety_wbtest.mbt` (proposals commit under drop
  and heal, with `committed_agrees`/`logs_consistent` invariants).
- `TestNetworkDelay` (delivery within a delay bound) → `set_delay` + the
  reordering/jitter paths exercised by `chaos_wbtest.mbt`.
- `TestBasicProgress` (5 nodes elect + commit 100 proposals) →
  `cluster_wbtest.mbt` / `property_wbtest.mbt` (elect + replicate + commit-agree).
- `TestRestart` / `TestPause` (crash/pause then resume, no split-brain) →
  `Cluster::crash` / `restart` in `chaos_wbtest.mbt` + the isolate/heal test in
  `network_safety_wbtest.mbt`.
The safety invariants these assert (one leader per term, committed logs agree)
are checked continuously by `sim_check.mbt`.

## raftpb/{confchange,confstate,raft}_test.go — DONE/N/A (2/3)
- `TestConfState_Equivalent` — DONE. `ConfState::equivalent` (set-equality of the
  four id lists + auto-leave) in `changer.mbt`; all cases in
  `confstate_wbtest.mbt`.
- `TestLeaveJoint` — DONE (behavioural part): `ConfChangeV2::is_leave`
  (empty batch ⇒ leave). Its `reflect`-based field-count guard is a Go
  struct-layout assertion — N/A (no protobuf wire structs here).
- `TestProtoMemorySizes` — N/A (Go struct memory-layout assertion).

## types_test.go — DONE (2/2)
Feature added: `EntryId` (with `Entry::id`) and `LogSlice`
(`valid`/`last_entry_id`/`last_index`) in `log.mbt`. Ported in
`types_port_wbtest.mbt`:
- `TestEntryID` — DONE (equality checks + all 3 `pbEntryID` cases).
- `TestLogSlice` — DONE (all 20 cases: dummy, prev-only, single, multi,
  first-entry-inconsistent, inconsistent-entries).

## util_test.go — DONE (5/5)
Feature added: `limit_size`, `entry_encoding_size`, `ents_size`, `payload_size`,
`describe_entry` (with Go-`%q`-style quoting) and `Payload::is_local` in
`log.mbt`. Ported in `util_port_wbtest.mbt`:
- `TestLimitSize` — DONE (all 6 cases + the "≥1 entry, else within budget"
  property). Sizes come from `entry_encoding_size` (a varint-based proto.Size
  analogue), used consistently for both the budget and the check, exactly as the
  Go test uses `proto.Size`.
- `TestPayloadSizeOfEmptyEntry` — DONE.
- `TestDescribeEntry` — DONE (both the default `%q` renderer, incl. the embedded
  NUL → `\x00`, and a custom uppercase formatter).
- `TestIsResponseMsg` — DONE (equivalent). Re-expressed over this port's typed
  `Payload` via `Message::is_response`, covering all 9 Payload kinds.
- `TestIsLocalMsg` — DONE (equivalent). This port has no flat `MessageType` enum
  and no node-local messages (etcd's MsgHup/MsgBeat/MsgSnapStatus/MsgCheckQuorum/
  MsgStorage* are direct method calls here, not `Payload`s), so `Payload::is_local`
  is false by construction; the test asserts this for every Payload kind.

## node_bench_test.go, rafttest/node_bench_test.go — N/A (benchmarks)

---

## Bug fixes found by ported tests
- **S1 (Storage snapshot semantics).** `MemoryStorage::apply_snapshot` used to
  advance the HardState commit index to the snapshot index; etcd's
  `ApplySnapshot` leaves HardState untouched (commit is managed separately).
  Fixed while aligning the Storage contract; `TestStorageApplySnapshot` and the
  storage round-trip tests pin the corrected behaviour.
- **S2 (compacted vs unavailable indistinguishable).** The old storage returned
  `None`/`false` for both "index compacted away" and "index past the end", so a
  caller could not tell whether to send a snapshot or wait. Replaced with the
  `StorageError` ADT (`Compacted`/`Unavailable`/`SnapOutOfDate`/
  `SnapshotTemporarilyUnavailable`) raised on the read path and caught in
  `storage_bridge.mbt`; `TestStorageTerm`/`TestStorageEntries`/`TestStorageCompact`
  assert the distinction.
- **B1 (Election Safety / State-Machine Safety).** `step` never dropped a
  response stamped with a term below ours, so a reordered stale `VoteResp` could
  be counted into a false majority (two leaders in one term) and a stale
  `AppendResp` could inflate a follower's progress and over-commit. Fixed by an
  up-front term classifier (`TermRel`) with an exhaustive match that drops stale
  responses. Regression tests in `b1_stale_term_wbtest.mbt` fail before the fix
  (`is_leader()` true; commit `3 != 0`) and pass after.

## `moon test` totals per batch
- Baseline (pre-porting): 143 passed.
- Batch 1 (inflights + quorum, unwired): 274 passed.
- Batch 2 (B1 fix + quorum wired into commit/vote + Inflights wired into the
  send/ack path + flow-control tests): **279 passed, 0 failed**, on all four
  backends (`--target all`: wasm, wasm-gc, js, native).
- Batch 3 (two-sided findConflictByTerm + committed-prefix early accept + 18
  raft_paper_test property tests incl. Figure 7 & Figure 8): **297 passed, 0
  failed**, on all four backends.
- Batch 4 (tracker/progress_test port): **302 passed, 0 failed**, on all four
  backends.
- Batch 5 (RawNode + Ready/Advance layer: `rawnode.mbt`, `ready.mbt`,
  `rawnode_driver.mbt`; 9/12 rawnode_test.go + 12/22 node_test.go ported, rest
  N/A with per-function reasons above): **325 passed, 0 failed** on wasm,
  wasm-gc, and js. Native `moon check --target native` is green; native `moon
  test` cannot run in this environment (missing C toolchain header `stddef.h`),
  which also fails identically on the untouched `master` baseline — an
  environment limitation, not a code regression.

## Batch (port-remaining): raft_test.go PORT/PARTIAL cleanup

New test files, all prefixed `port_`, adding **22** raft_test.go cases as `test`
blocks. Grep-reproducible counts:

```
$ grep -c '^test "' port_send_append_wbtest.mbt      # 4
$ grep -c '^test "' port_checkquorum_wbtest.mbt      # 6
$ grep -c '^test "' port_election_wbtest.mbt         # 4
$ grep -c '^test "' port_prevote_wbtest.mbt          # 4
$ grep -c '^test "' port_leader_election_wbtest.mbt  # 4
$ grep -h '^test "' port_*.mbt | wc -l               # 22
```

Census rows (GAP_core.md 118-register) moved PORT/PARTIAL -> COVERED this batch:

- #7  TestLeaderElectionPreVote                 (port_leader_election)
- #13 TestLeaderElectionOverwriteNewerLogs      (port_leader_election; was PARTIAL)
- #14 TestLeaderElectionOverwriteNewerLogsPreVote (port_leader_election)
- #22 TestDuelingCandidates                     (port_election)
- #23 TestDuelingPreCandidates                  (port_election)
- #42 TestCandidateResetTermMsgHeartbeat        (port_election)
- #43 TestCandidateResetTermMsgApp              (port_election)
- #48 TestLeaderStepdownWhenQuorumActive        (port_checkquorum)
- #50 TestLeaderSupersedingWithCheckQuorum      (port_checkquorum)
- #51 TestLeaderElectionWithCheckQuorum         (port_checkquorum)
- #52 TestFreeStuckCandidateWithCheckQuorum     (port_checkquorum)
- #53 TestNonPromotableVoterWithCheckQuorum     (port_checkquorum)
- #64 TestLeaderIncreaseNext                    (port_send_append; was PARTIAL)
- #65 TestSendAppendForProgressProbe            (port_send_append)
- #66 TestSendAppendForProgressReplicate        (port_send_append)
- #67 TestSendAppendForProgressSnapshot         (port_send_append; explicit table form)
- #102 TestLeaderTransferReceiveHigherTermVote  (port_prevote)
- #109 TestNodeWithSmallerTermCanCompleteElection (port_prevote)
- #110 TestPreVoteWithSplitVote                 (port_prevote)
- #111 TestPreVoteWithCheckQuorum               (port_prevote)

Plus two explicit table forms of rows already COVERED elsewhere: #6
TestLeaderElection and #49 TestLeaderStepdownWhenQuorumLost.

### Skipped (real defect, not a test weakness)
- #85 TestAddNodeCheckQuorum: commented out in `port_addnode_cq_wbtest.mbt`.
  Reproduces a divergence (FINDINGS_LEDGER.md #16): `reconcile_peers`
  (`raftnode.mbt`, off-limits to this test agent) builds a newly-added voter's
  `Progress` with `recent_active: false`, whereas etcd's `initProgress` sets it
  true, so a check-quorum leader steps down on the first quorum-check tick after
  an add. Assertions preserved (commented) for restoration once the source path
  is fixed.

### Still PORT/PARTIAL after this batch (with reasons)
- #61 TestLeaderAppResp / #118 TestLogReplicationWithReorderedMessage: require a
  `reject_index` field on the AppendEntries reply (a breaking cross-segment
  Payload wire change all segments deferred to 0.4.0). Reachable-case subset only.
- #79 TestSlowNodeRestore / #97 TestLeaderTransferAfterSnapshot: require snapshot
  delivery through `Ready` plus a message-hook harness; this port does not flow
  snapshots through `Ready` (rd.snapshot is always None), a 2-path property.
- #60 TestReadOnlyDuplicateRequest: requires a message-delay/duplicate hook the
  FIFO harness does not model.
- #113/#114 TestPreVoteMigration*: require a node's `pre_vote` to flip mid-test
  (mixed-version rolling restart); `RaftNode.pre_vote` is not a mutable field.
- #1 TestProgressLeader: requires the leader to appear in its own progress map
  plus async self-ack (2-path/AsyncStorageWrites), both source-level.
- #39 TestRecvMsgPreVote: the synthetic equal-term pre-vote grant needs review
  (`handle_pre_vote` grants only on strictly-higher term); left PARTIAL pending a
  divergence determination.
- #57 TestReadOnlyWithLearner: portable but needs a read-index injection harness;
  not built this batch.
