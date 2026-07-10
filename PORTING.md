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

## raft_paper_test.go — TODO (0/26)  [next batch]
Raft-paper Figure-2 property tests. Highest value. Being ported onto
`RaftNode`/`Node`.

## log_test.go — TODO (0/21)
Maps to `log.mbt` / `replication.mbt` (`term_at`, `entries_after`, `store_entries`,
`find_conflict_by_term`, commit/stable bookkeeping).

## log_unstable_test.go — IMPL (0/9)
Depends on the `unstable` log split (stable vs in-memory tail) which our model
does not separate yet. Tracked under the Ready/Advance storage work.

## tracker/progress_test.go — TODO (0/8)
Maps to `progress.mbt`. Some require the `Inflights` wiring and ADT progress
states (in progress).

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
