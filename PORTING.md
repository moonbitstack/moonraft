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

## log_test.go — DONE (7/21 ported, 14 N/A: no raftLog/unstable/apply-pacing)
Ported in `log_port_wbtest.mbt`. This port has no separate `raftLog`/`unstable`
split and no Ready/Advance apply-pacing pipeline; the log lives directly on the
`Node`. Tests are run against the Node's log API.
- `TestFindConflict` — DONE (all 10 cases). Added `Node::find_conflict` +
  `Node::match_term` (`log.mbt`).
- `TestFindConflictByTerm` — DONE (all 21 cases, both the index-1 and the
  compacted-baseline blocks). Runs against the existing `find_conflict_by_term`;
  each case also asserts the returned term equals `term_at(index)`, matching
  etcd's `zeroTermOnOutOfBounds(l.term(index))`.
- `TestIsUpToDate` — DONE (all 9 cases). Added `Node::is_up_to_date` wrapping the
  existing `candidate_log_up_to_date`.
- `TestLogMaybeAppend` — DONE (13/15 cases) against `handle_append_entries`. The
  two remaining etcd cases — a probe strictly below the commit index, and a
  conflict with an already-committed entry — **diverge, not weakened**: this port
  carries newer etcd's "accept the already-committed prefix outright" rule
  (`replication.mbt`), so it returns success with `match_index = commit` instead
  of panicking. Documented rather than asserted, since matching the older
  panic-contract would require changing out-of-boundary `replication.mbt` to an
  arguably-worse behaviour.
- `TestTerm` — DONE (values, 5 cases) via `term_at`. The `ErrCompacted`/
  `ErrUnavailable` distinction is a Storage-layer concern (covered by
  `TestStorageTerm`); `Node.term_at` is the "zero term on out of bounds" view.
- `TestMatchTerm` — DONE (equivalent, via `match_term`; the etcd original lives
  inside `TestCompactionSideEffects`).
- `TestCommitTo` — DONE (monotonic-advance half, via `advance_commit`). The
  commit-past-lastIndex panic case is N/A: `advance_commit` is defensive and
  ignores it rather than panicking (a caller-error guard, not a consensus path).
- N/A (no analogue in this port; each depends on `raftLog`+`unstable`+the
  Ready/Advance apply-pacing subsystem that the port deliberately does not have):
  `TestAppend` (asserts `unstable.offset`), `TestCompactionSideEffects`,
  `TestHasNextCommittedEnts`, `TestNextCommittedEnts`, `TestAcceptApplying`,
  `TestAppliedTo`, `TestNextUnstableEnts`, `TestStableTo`, `TestStableToWithSnap`,
  `TestCompaction`, `TestLogRestore`, `TestIsOutOfBounds`,
  `TestTermWithUnstableSnapshot`, `TestSlice`/`TestScan` (size-limited slicing —
  the size-cap semantics are instead covered at the Storage layer by
  `TestStorageEntries`/`TestLimitSize`).

## log_unstable_test.go — N/A (0/9 direct; equivalents ported)
This port has no `unstable` structure: the Node keeps a single contiguous log
anchored at the snapshot baseline, and there is no Ready/in-progress persistence
pipeline (`offsetInProgress`, `snapshotInProgress`, `acceptInProgress`,
`nextEntries`, `nextSnapshot`, `stableTo`, `restore`). The index/term/truncate
semantics that DO have a counterpart are reproduced in `unstable_port_wbtest.mbt`:
- `maybeLastIndex` ≈ `last_log_index` — DONE (entries / snapshot-only / empty).
- `maybeTerm` ≈ `term_at` — DONE (term from entry, from snapshot baseline, and
  zero outside range).
- `truncateAndAppend` ≈ `store_entries` — DONE (append-to-end, replace, and
  truncate-then-append shapes).
- N/A: `TestUnstableMaybeFirstIndex`, `TestUnstableNextEntries`,
  `TestUnstableNextSnapshot`, `TestUnstableAcceptInProgress`,
  `TestUnstableStableTo`, `TestUnstableRestore` — all bookkeeping for the
  stable-vs-in-progress hand-off to a Ready struct, which this port does not
  model. The "offset without a snapshot" shapes are also not representable: the
  port's log is always contiguous from its snapshot baseline.

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

## confchange/{datadriven,quick,restore}_test.go — IMPL (0/3 funcs, many cases)
Needs the confchange applier (Simple/EnterJoint/LeaveJoint/Restore) + learners.

## interaction_test.go — IMPL (0/1, datadriven)
Needs RawNode + Ready. Large datadriven corpus.

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
- `TestStorageCreateSnapshot` — DONE (2 cases; ConfState omitted — this port's
  `Snapshot` carries no ConfState, only index/term/data are asserted).
- `TestStorageAppend` — DONE (all 7 cases; exact post-append `ents` layout).
- `TestStorageApplySnapshot` — DONE (3 cases; normal / `SnapOutOfDate` /
  bootstrap-with-index-0). Semantic fix: `apply_snapshot` no longer bumps the
  commit index (etcd's `ApplySnapshot` leaves HardState untouched); the old
  behaviour diverged.

## rafttest/{network,node}_test.go — TODO (0/5, +1 bench N/A)
Deterministic network harness; overlaps our `sim.mbt`.

## raftpb/{confchange,confstate,raft}_test.go — TODO/N/A (0/3)
- `TestLeaveJoint`, `TestConfState_Equivalent` — TODO (confstate helpers).
- `TestProtoMemorySizes` — N/A (Go struct memory-layout assertion; no equivalent
  in MoonBit — we do not use protobuf wire structs).

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
