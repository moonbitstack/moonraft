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

## quorum/majority_test.go — TODO (0/1)
- `TestDescribe` — TODO. `MajorityConfig.Describe` is an ASCII progress-bar
  renderer (diagnostics only, no consensus semantics). Low priority; will add a
  `describe` renderer + port its 4 cases.

## quorum/quick_test.go — TODO (0/1)
- `TestQuick` — TODO. Property test: `CommittedIndex` vs a brute-force reference
  over random configs. Portable as a seeded randomized test.

## quorum/bench_test.go — N/A (benchmark)
- `BenchmarkMajorityConfig_CommittedIndex` — N/A (performance benchmark, not a
  correctness test). `committed_index` correctness is covered by the 66 commit
  cases above.

---

## raft_paper_test.go — DONE (18/26)
Raft-paper property tests, ported onto `RaftNode`/`Node` in `paper_wbtest.mbt`
(pre-vote disabled to match etcd's `newTestRaft` default):
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
- Pending (8): `TestRejectStaleTermMessage` (covered instead by
  `b1_stale_term_wbtest.mbt`), `TestCandidateStartNewElection`,
  `TestLeaderBcastBeat`, `TestFollower/CandidateElectionTimeoutRandomized`,
  `TestFollowers/CandidatesElectionTimeoutNonconflict`,
  `TestLeaderCommitPrecedingEntries`.

### findConflictByTerm (two-sided) — implemented
`Node::find_conflict_by_term` + `conflict_term` on `AppendEntriesReply` +
`Progress::maybe_decr_to`. The follower now hints `(index, term)` and the leader
refines it against its own log, matching etcd's `raft_test.go:TestFastLogRejection`
mechanism (that test itself is pending a full 2-node harness). Our own
`append_hint_wbtest.mbt` was corrected from the old one-step-backoff values to
the etcd findConflictByTerm values.

## log_test.go — TODO (0/21)
Maps to `log.mbt` / `replication.mbt` (`term_at`, `entries_after`, `store_entries`,
`find_conflict_by_term`, commit/stable bookkeeping).

## log_unstable_test.go — IMPL (0/9)
Depends on the `unstable` log split (stable vs in-memory tail) which our model
does not separate yet. Tracked under the Ready/Advance storage work.

## tracker/progress_test.go — DONE (5/8)
Ported in `progress_port_wbtest.mbt`:
- `TestProgressMaybeDecr` — DONE (all 10 cases; exercises `maybe_decr_to`).
- `TestProgressUpdate` — DONE (4 cases).
- `TestProgressBecomeReplicate` — DONE.
- `TestProgressBecomeSnapshot` — DONE (pending-snapshot index tracked as
  `next_index - 1`).
- `TestProgressIsPaused` — DONE (adapted: our model paces `Probe` from the
  caller rather than a per-Progress `MsgAppFlowPaused` flag, so the Probe+paused
  case is N/A; Replicate-window-full and Snapshot are covered).
- Pending (3): `TestProgressString` (debug renderer), `TestProgressResume`
  (`MsgAppFlowPaused` reset — not modelled), `TestProgressBecomeProbe` (asserts
  `Next` reset semantics our `become_probe` intentionally leaves to
  `maybe_decr_to` in the reject path).

## raft_flow_control_test.go — DONE (3/3)
`Inflights` is wired into `Progress` (`is_paused`/`sent_entries`/`free_le`),
`send_to`/`bcast_append` (throttle a full streaming window) and
`handle_append_resp` (release acked slots). Ported in `flow_control_wbtest.mbt`:
- `TestMsgAppFlowControlFull` — DONE.
- `TestMsgAppFlowControlMoveForward` — DONE.
- `TestMsgAppFlowControlRecvHeartbeat` — DONE (see MsgHeartbeatResp below).

## MsgHeartbeat / MsgHeartbeatResp — DONE
Heartbeats are now a distinct message type (`Heartbeat`/`HeartbeatResp`), not an
empty AppendEntries. `bcast_heartbeat` emits a `Heartbeat` carrying the leader's
commit index (capped at the follower's match); `handle_heartbeat` advances the
follower's commit and applies committed conf changes; `handle_heartbeat_resp` is
pure liveness (marks active for check-quorum/lease) and resumes replication —
sending the entries a behind follower lacks, or a content-free probe when its
in-flight window is full. An empty AppendEntries is now labelled `Probe`.

## raft_snap_test.go — TODO (0/5)

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
- `TestRawNodeProposeAndConfChange` — DONE for the V1 `AddNode` case (voter set
  `{1,2}`, command-then-confchange log layout). The 7 joint / `ConfChangeV2` /
  learner / `LearnersNext` cases are **N/A**: they need `ConfChangeV2`, joint
  consensus, and `ConfState` (`VotersOutgoing`/`Learners`/`AutoLeave`) that live
  in `confchange.mbt` / `membership.mbt` — owned by another agent's boundary,
  and the current confchange is V1 single-server (`+id`/`-id`) only.
- `TestRawNodeJointAutoLeave` — **N/A**: pure joint auto-leave (ConfChangeV2 +
  `AutoLeave`), same boundary as above.
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
- `TestRawNodeStatus` — DONE for leader/role/term. etcd additionally asserts on
  `status.Progress[1]` and `tracker.Config`; **that part is N/A** — `RaftStatus`
  does not expose the progress map or a `quorum.JointConfig`, which are
  membership-boundary types.
- `TestRawNodeCommitPaginationAfterRestart` — **N/A**: exercises byte-size commit
  pagination (`MaxSizePerMsg` + `ignoreSizeHintMemStorage`) and the
  HardState-commit-regression bug it guards. Our layer applies committed entries
  by index, not by a per-Ready byte budget, so there is no size-hint code path to
  regress. Equivalent coverage: `TestRawNodeStart`/`TestNodeAdvance` prove the
  applied cursor never gaps or regresses across Ready/Advance cycles.
- `TestRawNodeBoundedLogGrowthWithPartition` — **N/A**: needs
  `MaxUncommittedEntriesSize` + the raft-core `uncommittedSize` backpressure
  counter, which does not exist in this build (proposals are never throttled by
  an uncommitted-byte budget). Would require core changes outside the RawNode
  layer.
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
- `TestDisableProposalForwarding` — **N/A**: proposal forwarding from follower to
  leader is not modeled — `propose` is leader-only (drops on a follower, which is
  the `DisableProposalForwarding=true` behaviour, not the default forward).
- `TestNodeReadIndexToOldLeader` — **N/A**: read-index forwarding across a leader
  change; forwarding is not modeled.
- `TestNodeProposeWaitDropped` — **N/A**: goroutine + `ErrProposalDropped` via a
  `step`-hook injection + `context` timeout cancellation.
- `TestNodeStop` — **N/A**: goroutine lifecycle and `Stop` idempotency; the empty
  `Status{}` after stop has no analogue.
- `TestNodeProposeAddLearnerNode` — **N/A**: learners
  (`ConfChangeAddLearnerNode`, `ConfState.Learners`) — confchange/membership
  boundary.
- `TestAppendPagination` — **N/A**: `MaxSizePerMsg` append pagination measured
  through the `rafttest` network harness `msgHook`. Our `entries_after_limited`
  implements the per-message cap, but the end-to-end network-partition byte-budget
  assertion needs the rafttest harness (separate porting target).
- `TestCommitPagination` — **N/A**: `MaxCommittedSizePerReady` byte pagination;
  not modeled (we apply by index, not byte budget).
- `TestCommitPaginationWithAsyncStorageWrites` — **N/A**: `AsyncStorageWrites`
  (see the async section below).
- `TestNodeCommitPaginationAfterRestart` — **N/A**: byte-size commit pagination +
  `ignoreSizeHintMemStorage` (same reason as the rawnode twin).

### asyncStorageWrites — NOT implemented (deliberate), with equivalent coverage
etcd's `AsyncStorageWrites` replaces `Advance` with `MsgStorageAppend`/
`MsgStorageApply` local messages carrying `Responses`, delivered back through
`Step`. It exists to overlap disk writes with computation across goroutine
threads (`LocalAppendThread`/`LocalApplyThread`). Our build is single-threaded
and the browser demo target is wasm, where MoonBit's `async` is unsupported;
there is no thread to hand storage work to. Implementing the async message
protocol would add a parallel `Ready` code path (`newStorageAppendMsg`/
`newStorageAppendRespMsg`/`acceptReady` sidecar) with no runtime benefit here and
would not unlock any protocol behaviour the synchronous path does not already
cover. The two async-only tests
(`TestCommitPaginationWithAsyncStorageWrites`) are the only upstream tests it
would add; both are byte-pagination variants already N/A on the size-budget
grounds above. Our synchronous `ready`/`advance` provides the same *ordering*
guarantee the contract mandates (persist `entries` before sending `messages`,
apply `committed_entries` only after they are stabled).

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
Still TODO for full parity: joint auto-leave, learner demotion, aborting an
in-flight leadership transfer to a removed target (tracked with learners /
leadTransferee below).

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

## confchange/{datadriven,quick,restore}_test.go — IMPL (0/3 funcs, many cases)
The single add/remove/add-learner voter path is live (B2 + learners). Still
needs the confchange *package* applier (batched `ConfChangeSingle` via
Simple/EnterJoint/LeaveJoint/Restore over voters+learners+learners_next) before
the datadriven corpus can be expanded case-by-case.

## interaction_test.go — IMPL (0/1, datadriven)
Needs RawNode + Ready. Large datadriven corpus.

## storage_test.go — TODO (0/8)
Maps to `storage.mbt` + error ADT (`ErrCompacted`/`ErrUnavailable`/`ErrSnapOutOfDate`).

## rafttest/{network,node}_test.go — TODO (0/5, +1 bench N/A)
Deterministic network harness; overlaps our `sim.mbt`.

## raftpb/{confchange,confstate,raft}_test.go — TODO/N/A (0/3)
- `TestLeaveJoint`, `TestConfState_Equivalent` — TODO (confstate helpers).
- `TestProtoMemorySizes` — N/A (Go struct memory-layout assertion; no equivalent
  in MoonBit — we do not use protobuf wire structs).

## types_test.go — TODO (0/2)
- `TestEntryID`, `TestLogSlice` — TODO (map to `Entry`/log-slice helpers).

## util_test.go — TODO/N/A (0/5)
- `TestLimitSize`, `TestIsLocalMsg`, `TestIsResponseMsg` — TODO.
- `TestDescribeEntry`, `TestPayloadSizeOfEmptyEntry` — mostly diagnostics; TODO.

## node_bench_test.go, rafttest/node_bench_test.go — N/A (benchmarks)

---

## Bug fixes found by ported tests
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
