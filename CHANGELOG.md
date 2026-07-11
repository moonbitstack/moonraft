# Changelog

Follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project
adheres to semantic versioning.

## [0.5.0]

Closes the transliteration of `etcd-io/raft@26647d5`: the porting census
(`PORTING.md`) has no `PARTIAL` / `TODO` / `PORT-pending` rows left — only `DONE`
and the three acceptable `N/A` kinds (Go-concurrency shells, benchmarks, Go
struct-memory-layout asserts), each with a named equivalent. 723 tests pass on
all four backends; line and branch coverage are both 100% (3094/3094). The
differential trace against upstream was re-verified with no MATCH→DIVERGE
regression, and the `reject_index` fix is now positively reproduced (reverting it
diverges, restoring it returns to a frame-identical trace).

Includes the `0.4.1` `reject_index` change below.

### Added

- **Snapshot delivery through `Ready`.** A leader-installed snapshot now surfaces
  through the driver's `Ready`/advance cycle: `RawNode::next_unstable_snapshot`
  fills `Ready.snapshot` (etcd's `rawnode.go` `nextUnstableSnapshot`), and it is
  acknowledged with `stable_snap_to` on store/advance. Closes the port of
  `TestSlowNodeRestore` (#79) and `TestLeaderTransferAfterSnapshot` (#97).
- **`RawNode::set_pre_vote`.** `pre_vote` is now a runtime-mutable field, so a
  mixed-version cluster can enable pre-vote node by node (etcd's
  `newPreVoteMigrationCluster`). Ports `TestPreVoteMigration*` (#113/#114).
- **The leader now appears in its own progress map** with a synchronous self-ack,
  matching etcd's `reset()`/`becomeLeader`. Ports `TestProgressLeader` (#1).

### Fixed

- **Same-term pre-votes are now granted per etcd `canVote`.** `handle_pre_vote`
  granted only on a strictly-higher term, silently rejecting every same-term
  pre-vote; it now also grants when it has already voted for the candidate, or
  when it has neither voted nor recognised a leader this term (`raft.go:1214`).
  Ports `TestRecvMsgPreVote` (FINDINGS_LEDGER #23).
- **A lone voter no longer stalls a read on a down learner.** The read-index
  singleton fast path was gated on the peer count rather than etcd's voters-only
  `IsSingleton`, so a single voter with a learner would, under `ReadOnlySafe`,
  broadcast a heartbeat and wait for the learner to ack. It now takes the fast
  path whenever the voter set is a singleton. Ports `TestReadOnlyWithLearner` and
  `TestReadOnlyDuplicateRequest` (#57 / #60; FINDINGS_LEDGER #24).

## [0.4.1]

A single breaking wire change that completes the port of the AppendEntries
rejection path.

### Added (breaking)

- **`AppendEntriesReply.reject_index`.** The reply now carries the probe point a
  follower rejected — etcd's `MsgAppResp.Index` on a rejection (`raft.go:1828`,
  `Index: m.GetIndex()`). Adding a field to the `pub(all)` reply struct is a
  breaking change for any code constructing or exhaustively matching it.

### Fixed

- **Reordered rejections no longer drive a spurious back-off.** The leader fed
  `maybe_decr_to` a `rejected` value re-derived as `next_index - 1` instead of
  the index the follower actually rejected, which made that function's staleness
  guard (`next_index - 1 != rejected`) always false — dead code. It now feeds the
  carried `reject_index`, so a reordered reject for an entry no longer in flight
  is discarded rather than regressing the follower's progress. Verified by ports
  of upstream `TestLeaderAppResp` and `TestLogReplicationWithReorderedMessage`
  (FINDINGS_LEDGER #22).

## [0.4.0]

A coordinated breaking-change release: the codebase is reorganised into packages,
the source is regularised toward idiomatic MoonBit, and two self-introduced
deviations from `etcd-io/raft` are removed so the behaviour matches upstream.

### Changed

- **Directory layout.** The flat source tree is split into eight packages that
  mirror the upstream `etcd-io/raft` layout (`quorum`, `tracker`, `raftpb`,
  `confchange`, `storage`, `log`, `core`, `demo`) behind a root `raft.mbt`
  facade. Consumers are unaffected: `moon add Lfan-ke/raft-moonbit` and the
  `@raft.X` usage in the README are unchanged, verified against an external
  consumer module.
- **Idiomatic-MoonBit polish** across all packages: `is`-guards, `guard … else`
  early returns, or-patterns, `Option::map` / `unwrap_or`. A differential trace
  against `etcd-io/raft@26647d5` confirms zero behaviour drift from both the
  restructure and the polish.

### Removed (breaking)

- **The self-invented 256-entry per-`MsgApp` cap.** `etcd-io/raft` caps an append
  message by bytes only (`sendAppend` uses `entries(pr.Next, maxMsgSize)`); the
  extra entry-count limit was ours. `Config.max_entries` and the `max_entries`
  parameter of `RaftNode::new` are removed; appends are now bounded solely by
  `max_msg_bytes`, matching upstream.
- **The self-invented `SnapshotResp` message.** In `etcd-io/raft` a follower
  answers `MsgSnap` with an `MsgAppResp` (`handleSnapshot`, carrying
  `Index = lastIndex()` on restore or `Index = committed` when the snapshot is
  stale); there is no dedicated snapshot-response type. The `SnapshotResp`
  variant is removed and the response is unified onto the append-reply path, so
  the stale-term filter covers it without a separate route.

### Fixed

Twenty-one correctness defects surfaced during the port were fixed under
red-then-green regression tests; see `PORTING.md` and the differential report.
Line and branch coverage reach 100%.

## [0.3.0]

### Added

- `ConfChangeV2` with the `Transition` field and the simple-apply path.
- Joint consensus `C(old,new)`, learners, and staged demotion via `learners_next`.

## [0.2.0]

### Added

- `learners_next` tracked in the live `Membership` (deferred demotion, §4.3).
- The `unstable` / `RaftLog` split with in-progress bookkeeping, `RawNode` and
  the `Ready` / `advance` cycle.

### Fixed

- The uncommitted-tail quota is released at apply time, not commit time.

## [0.1.0]

Initial release: leader election, log replication with conflict backoff,
commit advancement, the up-to-date-log safety restriction, pluggable
`StateMachine` / `Transport` / storage traits, and a deterministic simulator.
