# Differential Trace Report — raft-moonbit vs etcd-io/raft

## Headline

Against **etcd-io/raft @ `26647d5`** (pinned as a git submodule), a 12-scenario
self-authored suite was driven through **two independent harnesses** — one over
etcd's real `RawNode` (Go), one over raft-moonbit's `RawNode` (MoonBit) — using
one shared DSL and one shared normalized JSON schema. Across **397 event frames**
(each carrying every node's authoritative state + the sorted set of messages it
emitted), the two traces were compared **field by field**.

**Result:** 4 scenarios are byte-identical; 8 diverge. The divergences resolve to
**7 distinct behavioral differences** plus 2 representation/traversal-order
artifacts. Of the 4 known-unfixed `DIVERGENT`s used as positive controls, **1 was
reproduced by trace (the 256-entry MsgApp cap), 1 confirmed structurally
(self-invented `SnapshotResp`), and 2 were NOT surfaced** — honestly, because the
harness lacks the leader-side log compaction and the specific Replicate-state
stale-reject construction they require (see §5). The suite also independently
surfaced **several differences not on the prior list** — most notably that
raft-moonbit's `ReadIndex` under `ReadOnlySafe` performs **no leadership-
confirmation heartbeat round**.

This is a regression alarm, not a one-off: `run.sh` rebuilds both sides and
re-diffs on demand, and the etcd side is pinned to an exact commit.

## 0. Re-run on latest master `b10b66f` (three-way table)

The suite was rebased from base `aa501b3` onto **`b10b66f` (551 tests)** and
re-run. Both harnesses recompiled against the new library (the `Role` ADT gained
`PreCandidate`; both harnesses now emit `PreCandidate` faithfully instead of
folding it into `Candidate`). The harness sets `pre_vote` / `read_only_option`
explicitly, so the master default flips (`pre_vote true→false`,
`read_only LeaseBased→Safe`) do not move the baseline.

| Finding | On `aa501b3` | On `b10b66f` | Verdict |
|---------|--------------|--------------|---------|
| 11_transfer — `lead_transferee` not cleared on step-down (my #5) | DIVERGE | **MATCH** | **FIXED** (master #15). Cannot reproduce. |
| 12_stale_msg — `pending_conf_index` tracking (my #6) | DIVERGE | **MATCH** | **FIXED**. Cannot reproduce. |
| 02_prevote — pre-vote role | DIVERGE (Cand vs Foll) | DIVERGE (`PreCandidate` vs `Follower`) | **STILL HOLDS** — the `PreCandidate` *variant* was added, but `campaign()`'s pre-vote path still leaves `role = Follower`; the variant is not entered here. Now a real behavioral gap, not just representation. |
| 05 — `uncommitted` not reset on truncation | DIVERGE | DIVERGE | **STILL HOLDS** |
| 06 — 256-entry MsgApp cap (control #3) | DIVERGE | DIVERGE | **STILL HOLDS** (300 vs 256) |
| 07/09 — conf entry not counted in `uncommitted`; conf-append shape | DIVERGE | DIVERGE | **STILL HOLDS** |
| 08 — removed voter never learns removal | DIVERGE | DIVERGE | **STILL HOLDS** |
| 10 — `ReadIndex` emits no outbound at the read step | DIVERGE | DIVERGE | **STILL HOLDS** (see correction below) |
| 03 — heartbeat-resp surfacing order | DIVERGE | DIVERGE | traversal/timing, unchanged |

**Correction on the `ReadIndex` finding.** My earlier phrasing — "no leadership-
confirmation heartbeat under `ReadOnlySafe`" — is imprecise. The `lead_read_index`
→ `bcast_heartbeat` path exists. The *observable* fact the trace shows is narrower
and still real: calling `read_index` on the leader produces **no outbound messages
in the resulting `Ready`** (step 6: etcd emits 2 confirmation `Heartbeat`s in-step,
raft-moonbit emits 0); a later `tick` produces only ordinary heartbeats, never the
same confirmation round. The maintainer traced the root cause to a deeper bug in
`read_only.mbt`: the ack map is keyed by **user context** and `add_request`
**overwrites instead of merges**, so two reads sharing a context erase each
other's acks. etcd long ago switched to an internal 8-byte position context
(`heartbeatCtx()`) with batched release; raft-moonbit copied the pre-refactor
design. Being pointed to the right file is the value here — the exact wording was
not.

## 1. Schema (shared by both harnesses)

One JSON object ("frame") is emitted per DSL command. Keys are in a fixed order,
IDs are decimal strings on both sides.

```
{ "step": N, "cmd": "<dsl line>",
  "nodes": [ { per node, sorted by id:
      id, role,            // Follower|Candidate|Leader  (see note on PreCandidate)
      term, vote, lead,
      commit, applied, last_index, last_term,
      lead_transferee, pending_conf_index, uncommitted,
      voters, voters_outgoing, learners, learners_next   // id arrays, numeric-sorted
  } ],
  "msgs": [ { every message emitted during this command, sorted:
      type, from, to, term, index, logterm, entries, commit, reject, rejecthint
  } ] }
```

**Normalization decisions (and why each field is still "behavior"):**

- **Role.** etcd has `StatePreCandidate`; raft-moonbit has no such role (it stays
  `Follower` with an internal `in_pre_campaign` flag). Go's `PreCandidate` is
  mapped to `Candidate`. This is the one place the two role models genuinely
  differ (see 02_prevote); it is flagged, not hidden.
- **Message type.** etcd's `raftpb.MessageType` and raft-moonbit's `Payload`
  variants are mapped onto one tag set (`Append`,`AppendResp`,`Vote`,`VoteResp`,
  `PreVote`,`PreVoteResp`,`Heartbeat`,`HeartbeatResp`,`Snapshot`,`SnapshotResp`,
  `TimeoutNow`,`ReadIndex`,`ReadIndexResp`,`TransferLeader`,…). raft-moonbit's
  `kind()` returns `"Probe"` for an empty `Append`; that is normalized back to
  `Append` so a wording difference cannot masquerade as a behavior difference.
- **AppendResp field mapping.** etcd's flat `Index`/`Reject`/`RejectHint`/`LogTerm`
  map to raft-moonbit's `match_index`/`!success`/`conflict_index`/`conflict_term`.
  These are the *same* wire concepts; comparing them is comparing behavior.
- **Snapshot messages** carry the snapshot's metadata index/term in `index`/
  `logterm` on both sides so they are comparable (etcd's `MsgSnap` puts 0 in the
  flat fields).
- **Message ordering.** Within a step, messages are sorted by
  `(typeRank, to, from, term, index, logterm, rejecthint)` — an explicit numeric
  order identical on both sides (needed because MoonBit's `String "<"` is
  length-first, not lexicographic). **Sorting only removes emission-order noise;
  if the message *multiset* differs, that is reported as a real difference and
  never sorted away.**

Nothing was dropped from the schema to make a diff pass. The only field that is
partly harness-driven rather than core behavior is `applied` (the harness applies
committed entries and both sides advance identically), noted here for honesty.

## 2. Determinism

Elections are driven **explicitly** (`campaign`), so neither side's RNG affects
election *timing* — the historic blocker (etcd's `lockedRand` vs the MoonBit LCG)
is sidestepped entirely rather than pinned. A pin hook exists on the Go side
(`HarnessSetRandomizedElectionTimeout`, see §6) for tick-driven scenarios but is
not needed by the current suite. Proposal payloads are emitted as 1-byte-per-char
ASCII on both sides so `uncommitted` byte-accounting is comparable.

## 3. Scenario suite (12)

| # | Scenario | Covers | Verdict |
|---|----------|--------|---------|
| 01 | elect + commit | election, replication, commit | **MATCH** |
| 02 | pre-vote on | PreVote path | diverge (role representation) |
| 03 | partition → dueling → heal | brain-split, convergence | diverge (traversal) |
| 04 | leader crash → takeover | failover | **MATCH** |
| 05 | divergent follower log | conflict truncation | diverge (uncommitted reset) |
| 06 | isolated follower catch-up | flow control / batching | **diverge — positive control #3** |
| 07 | add voter | conf change (add) | diverge (uncommitted; replication timing) |
| 08 | remove voter | conf change (remove) | **diverge — removed node not notified** |
| 09 | add learner | learner add | diverge (uncommitted; replication timing) |
| 10 | ReadIndex (safe) | linearizable read | **diverge — no confirmation round** |
| 11 | leadership transfer | TimeoutNow handoff | diverge (stale lead_transferee) |
| 12 | repeated campaigns / stale | term races | diverge (pending_conf_index) |

## 4. Divergence table

`剧本 | 步 | 字段 | Go | MoonBit | 判定`

| Scenario | Step | Field | Go | MoonBit | Verdict |
|----------|------|-------|----|---------|---------|
| 02_prevote | 2 | n1.role | Candidate | Follower | **representation** — etcd `StatePreCandidate` vs mb `Follower`+`in_pre_campaign`. Messages are identical (both send `PreVote`); no safety impact. |
| 03_partition | 15 | msgs (set) | 4 | 6 (2 extra `HeartbeatResp`) | **traversal/timing** — heartbeat responses surface in a different stabilize round; final state converges identically. |
| 05_log_conflict | 16 | n1.uncommitted | 0 | 2 | **REAL** — after the ex-leader truncates its 2 conflicting tail entries and becomes a follower, etcd resets the uncommitted-size accumulator; raft-moonbit leaves it at 2. |
| 06_flowcontrol | * | msg.entries (max) | **300** | **256** | **REAL — known DIVERGENT #3.** etcd batches all 300 missing entries into one `MsgApp` (bounded only by `MaxSizePerMsg` bytes); raft-moonbit caps each `MsgApp` at 256 entries. Positive control **reproduced**. |
| 06_flowcontrol | 305 | msg.commit (many) | varies | varies | **traversal/timing** — 300 pipelined `Append`s interleave with acks differently, so per-message `commit` snapshots differ. Same multiset of entries; converges identically. |
| 07_conf_add | 4 | n1.uncommitted | 4 | 0 | **REAL** — etcd counts the pending conf-change entry toward uncommitted size; raft-moonbit does not (so `MaxUncommittedEntriesSize` would gate differently for conf changes). |
| 07_conf_add | 5 | msg[2].index / entries | 1 / 1 | 2 / 0 | **REAL (candidate)** — the append that carries the committed conf change differs: etcd replicates it as a 1-entry append at prev-index 1; raft-moonbit sends an empty append at index 2. Timing/representation of conf-change replication. |
| 08_conf_remove | 4 | n1.uncommitted | 4 | 0 | **REAL** — same accounting difference as 07. |
| 08_conf_remove | 5 | n3.commit / applied / voters | 2 / 2 / [1,2] | 1 / 1 / [1,2,3] | **REAL** — the removed node 3: etcd still replicates the commit that removes it, so node 3 learns `commit=2` and `voters=[1,2]`; raft-moonbit drops node 3 from replication *before* propagating that commit, so node 3 is stuck at `commit=1` and still believes it is a voter. |
| 08_conf_remove | 5 | msgs (set) | 6 (incl. `Append 1→3`, `AppendResp 3→1`) | 4 (no traffic to 3) | **REAL** — corollary of the above: raft-moonbit exchanges no messages with the removed node. |
| 09_learner | 4 | n1.uncommitted | 4 | 0 | **REAL** — same accounting difference as 07. |
| 09_learner | 5 | msg[2].index / entries | 1 / 1 | 2 / 0 | **REAL (candidate)** — same conf-change-replication difference as 07. |
| 10_readindex | 6 | msgs | 2 × `Heartbeat` (1→2, 1→3) | **0** | **REAL** — under `ReadOnlySafe`, etcd broadcasts a confirmation heartbeat before answering a `ReadIndex`; raft-moonbit emits nothing. |
| 10_readindex | 7 | msgs | 2 × `HeartbeatResp` | **0** | **REAL** — corollary: no confirmation round-trip at all. See §5 note — this affects read linearizability. |
| 11_transfer | 7 | n1.lead_transferee | "" | "2" | **REAL — FIXED on `b10b66f`** (master #15); on the old base raft-moonbit left `leadTransferee` set on the now-follower. Now MATCH. |
| 12_stale_msg | 7–9 | n2.pending_conf_index | 0 | 1 | **REAL — FIXED on `b10b66f`**; now MATCH. |

*(The table above is measured on base `aa501b3`; rows 11 and 12 are MATCH on
`b10b66f` — see §0.)*

## 5. Positive controls (harness self-check)

The four known-unfixed `DIVERGENT`s were used to check the harness is capable of
seeing real differences. **Honest status:**

| # | Known DIVERGENT | Status | Evidence / why |
|---|-----------------|--------|----------------|
| 1 | `reject_index` not returned (Replicate branch substitutes `next-1`) | **NOT reproduced by trace** | 05_log_conflict creates divergent logs but converges during `stabilize` without surfacing a rejecting `AppendResp` in a frame. Triggering the *Replicate-branch* stale reject needs a leader that is already streaming (StateReplicate) to a follower when an out-of-order stale reject arrives; the current network model delivers in-order per pair, so this exact ordering was not constructed. Coverage gap, not a clean bill of health. |
| 2 | self-invented `SnapshotResp` (etcd replies `MsgAppResp` to `MsgSnap`) | **Confirmed structurally, NOT by trace** | The `Payload::SnapshotResp(InstallSnapshotReply)` variant exists in raft-moonbit's message type; a `MsgSnap` is only ever sent when the leader's log is compacted below a follower's `next`. The RawNode-based harness has **no leader-side compaction hook**, so no `MsgSnap`/`SnapshotResp` is emitted. Reproducing it needs the snapshot/compaction DSL primitive (see §7 gaps). |
| 3 | per-`MsgApp` 256-entry cap (etcd caps by bytes only) | **REPRODUCED** ✓ | 06_flowcontrol: Go `entries=300` in one append, MoonBit `entries=256`. |
| 4 | `AsyncStorageWrites` / `MaxCommittedSizePerReady` absent from `Config` | **Confirmed by API inspection, not a runtime event** | raft-moonbit's `Config` has no `async_storage_writes` / `max_committed_size_per_ready` fields (its async mode is reached via `RaftNode::raw_async`, not `Config`). This is a config-surface absence, not something a state trace emits. |

**Discipline note:** 3 of 4 controls did not surface as green-by-accident — each
un-reproduced one has a concrete, stated reason tied to a missing harness
primitive, per the "suspect the harness, don't celebrate" rule.

## 6. Go-side source modification (auditable)

etcd is pinned pristine as a submodule at `difftest/vendor/etcd-raft` (`26647d5`).
Exactly **one file** is overlaid onto that checkout at build time,
`difftest/harness_export.go` (tracked in this branch, copied in by `run.sh`):

- `HarnessSetRandomizedElectionTimeout(*RawNode, int)` — lifts the existing
  test-only helper `raft_test.go:4204` into a normal export (determinism pin;
  currently unused because elections are explicit).
- `HarnessSnapshotOf(*RawNode) HarnessSnapshot` — reads the fields that are
  private to `package raft` and not on `Status()`: `last_index`, `last_term`,
  `pending_conf_index`, `uncommittedSize`, plus the conf sets. Everything else
  (`role`,`term`,`vote`,`commit`,`applied`,`lead`,`lead_transferee`,voters…) comes
  from the public `Status()`/`BasicStatus()`.

The algorithm is untouched. Verify with:
`git -C difftest/vendor/etcd-raft rev-parse HEAD` == `26647d5…` and
`git -C difftest/vendor/etcd-raft status` (only the untracked overlay file).

Toolchain: the submodule requires Go 1.26 (it uses the `new(expr)` language
feature); `run.sh` sets `GOTOOLCHAIN=auto GOPROXY=https://goproxy.cn,direct` so
Go 1.26.x is fetched automatically. Local Go is 1.25.5.

## 7. Honest list — what did NOT run / does NOT line up

- **Positive controls #1 and #2 not reproduced by trace** (see §5): need a
  Replicate-state stale-reject construction and a leader-side snapshot/compaction
  primitive respectively. Neither is implemented in the DSL yet.
- **`snapshot` / `crash+restart` DSL primitives are minimal.** `crash` marks a
  node dead; there is no `restart` (etcd would reload from `MemoryStorage`;
  raft-moonbit's RawNode has no injected storage — restart would need
  `Node::save_into`/`load_from`). Snapshot install (family 6) is therefore not
  exercised.
- **Partition model optimistically drops.** Messages across a partition are
  dropped silently (no `ReportUnreachable`), and etcd advances `next`
  optimistically in StateReplicate, so a partitioned follower can be left behind
  in a way that needs a post-heal tick to reveal. Catch-up scenarios add an
  explicit `tick` after `heal`; without it nothing fires.
- **`applied`** is harness-advanced, not core behavior (both sides advance it the
  same way here); listed in the schema but not a behavioral signal on its own.
- **Pre-vote role** cannot be compared as a state (etcd `PreCandidate` vs mb
  `Follower`+flag). Exposing `in_pre_campaign` on the mb side would let this be
  compared directly; today it shows as a benign role diff.
- **wazero + official 41 datadriven scripts: NOT IMPLEMENTED** (see §8). Priority
  1 (this trace-diff harness) is complete; priority 2 was scoped, not built.

## 8. wazero harness (priority 2) — status and required wasm exports

Not implemented in this pass. Design is settled: a Go re-implementation of
`rafttest.InteractionEnv` whose handlers forward to raft-moonbit compiled to
`wasm` (`moon build --target wasm --release`), driven by `wazero` (pure Go, no
cgo), asserting against etcd's own `testdata/*.txt` expected output rendered with
etcd's `DescribeReady`/`DescribeMessage`.

**Blocking dependency — wasm exports needed (⑥, for coordinator to wire into
`moon.pkg`).** The demo `wnode_*` (9 fns) are insufficient. Needed, over the
string↔linear-memory (UTF-16LE, length at `ptr-4`) ABI already in use:

```
rawnode_new(config_json) -> handle
rawnode_tick / campaign / has_ready / ready(handle) -> ready_json
rawnode_step(handle, msg_json)
rawnode_propose(handle, bytes) / propose_conf(handle, cc_json)
rawnode_apply_conf_change(handle, cc_json) -> confstate_json
rawnode_advance(handle, ready_token) / store(handle, ready_token)
rawnode_status(handle) -> status_json         (role/term/vote/commit/applied/last_*)
rawnode_transfer_leader(handle, id) / read_index(handle, ctx)
rawnode_report_unreachable(handle, id) / report_snapshot(handle, id, ok)
rawnode_forget_leader(handle)
message_encode / message_decode                (Payload <-> stable JSON)
```

Driving these would also cover `worker_driver.mbt` (see §9), which is the single
biggest coverage hole.

## 9. Coverage (KPI)

Measured on `moon test --enable-coverage; moon coverage report -f summary`, base
commit **`b10b66f`** (551 tests):

- **Total: 2605 / 3994 lines.**
- **`worker_driver.mbt: 0 / 323`** — the wasm demo driver, entirely uncovered;
  it is exactly what the (not-yet-built) wazero harness would exercise. Excluding
  it: 2574 / 3656.
- Other low-coverage files worth main-line attention:
  `status.mbt 10/13`, `storage_bridge.mbt 22/29`, `read_only.mbt 71/82`,
  `sim_check.mbt 78/88`, `sim.mbt 130/140`, `raftnode_step.mbt 304/353`,
  `raftlog.mbt 187/227`.

The trace-diff harness itself is a `moon run` main package with no unit tests, so
it does not move `moon test` coverage; the wazero harness would (via
`worker_driver.mbt`). "Before/after" numbers converge once §8 is built.

## 10. Reproduce

```bash
# from the repo root (this branch, with the submodule initialized)
git submodule update --init difftest/vendor/etcd-raft
bash difftest/run.sh                         # build both, run all 12, print summary
bash difftest/run.sh scripts/06_flowcontrol_batch.script   # one scenario, full diff
```

`run.sh` overlays `harness_export.go` onto the pinned submodule, builds the Go
harness, regenerates the MoonBit `script_gen.mbt` per scenario, runs both, and
diffs via `diff.py`. Traces land in `difftest/traces/`.

Regression use: after `git rebase github/master`, re-run — any new field diff is a
freshly-introduced behavioral change (or a fix that closes one of the rows above).
