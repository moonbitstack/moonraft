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

## raft_flow_control_test.go — DONE (2/3)
`Inflights` is wired into `Progress` (`is_paused`/`sent_entries`/`free_le`),
`send_to`/`bcast_append` (throttle a full streaming window) and
`handle_append_resp` (release acked slots). Ported in `flow_control_wbtest.mbt`:
- `TestMsgAppFlowControlFull` — DONE.
- `TestMsgAppFlowControlMoveForward` — DONE.
- `TestMsgAppFlowControlRecvHeartbeat` — TODO. Needs a distinct MsgHeartbeatResp
  path (our model folds heartbeat into an empty Append), so the "heartbeat resp
  frees one slot" behaviour has no message to hang off yet. Tracked with the
  heartbeat/read-only work.

## raft_snap_test.go — TODO (0/5)

## raft_test.go — IMPL/TODO (0/118)
The core file. Many need features being implemented: learners, ReadOnlySafe,
leadTransferee state machine, confchange application. Ported incrementally.

## node_test.go — IMPL (0/22)
Needs the `Node`/`Ready`/`Advance` async API (RawNode layer).

## rawnode_test.go — IMPL (0/12, +2 benchmarks N/A)
Needs `RawNode` + `Ready`/`HasReady`/`Advance`/`acceptReady`.

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
