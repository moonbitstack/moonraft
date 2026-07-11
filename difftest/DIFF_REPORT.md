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

## 0.1 Final audit on master `5fce34a` (post-restructure regression sweep)

The differential framework (its 3 commits) was rebased from the `b10b66f`
baseline onto **`5fce34a`** — 56 commits later. Those 56 commits are a directory
restructure (the flat root `.mbt` corpus folded into 9 sub-packages `quorum /
tracker / raftpb / confchange / storage / log / core` behind a single root
`raft.mbt` facade), an idiomatic-MoonBit style regularization, and 20 defect
fixes. The rebase touched only `difftest/` plus a one-line `.gitignore` union
(both sides appended); the old flat `.mbt` files are gone (master authoritative),
only the `difftest/` subtree is carried on top. Main-library gate after
`moon clean`: **708 / 708 tests**, `moon check --deny-warn --target all` clean on
all four backends. The MoonBit harness recompiled **unchanged** against the
restructured facade — the `@raft.*` public contract survived the repackaging, so
no harness edit and no main-library edit were needed. The etcd submodule stays
pinned at `26647d5` with only the untracked `harness_export.go` overlay.

**Result: 8 scenarios MATCH, 4 diverge. No scenario regressed from MATCH to
DIVERGE.** Every field diff observed after the rebase was already present, byte
for byte, in the `b10b66f` baseline; several baseline diffs were *eliminated* by
the fixes. The restructure and style pass introduced **zero observable behavior
drift** — this is the independent confirmation the "no behavior drift" claim
required.

| # | Scenario | `b10b66f` | `5fce34a` | Change | Nature of any residual diff |
|---|----------|-----------|-----------|--------|-----------------------------|
| 01 | elect + commit | MATCH | **MATCH** | — | — |
| 02 | pre-vote on | DIVERGE (role) | **MATCH** | **fixed** | mb now enters `PreCandidate` on the campaign pre-vote path (both sides `PreCandidate`). |
| 03 | partition → heal | DIVERGE | **DIVERGE** | unchanged | traversal-order only: 2 MB-only `HeartbeatResp` surface a round earlier (Map iteration); state converges identically. |
| 04 | leader crash | MATCH | **MATCH** | — | — |
| 05 | divergent-log truncation | DIVERGE (uncommitted) | **MATCH** | **fixed** | uncommitted-size accumulator now reset on truncation. |
| 06 | flow control / batching | DIVERGE | **DIVERGE** | unchanged | the 256-entry `MsgApp` cap (etcd 300 in one append, mb 256 + a 44-entry follow-up); the single extra append shifts every downstream index, producing the ~256 `.commit` cascade + 2 traversal `HeartbeatResp`. Same as baseline. |
| 07 | add voter | DIVERGE ×2 | **DIVERGE ×1** | **improved** | uncommitted conf-accounting diff gone; only the conf-change replication *shape* remains (etcd 1-entry append at prev-index 1 vs mb empty append at index 2). |
| 08 | remove voter | DIVERGE (REAL) | **MATCH** | **fixed** | `pending_farewell` lands: removed node 3 now learns `commit=2`, `voters=[1,2]`, and the same 6-message exchange as etcd. |
| 09 | add learner | DIVERGE ×2 | **DIVERGE ×1** | **improved** | same as 07: accounting fixed, conf-change append shape remains. |
| 10 | ReadIndex (safe) | DIVERGE (REAL) | **MATCH** | **fixed** | leadership-confirmation heartbeat round now emitted on `read_index`. |
| 11 | leadership transfer | MATCH | **MATCH** | — | (already fixed at `b10b66f`) |
| 12 | repeated campaigns / stale | MATCH | **MATCH** | — | (already fixed at `b10b66f`) |

The 4 residual divergences reduce to **three** distinct causes, all pre-existing
and all documented in §4: (a) heartbeat-response Map-iteration order (03, and the
tail of 06) — representation, not behavior; (b) the 256-entry per-`MsgApp` cap
(06) — the standing known-DIVERGENT #3; (c) the conf-change replication shape
(07, 09) — a timing/representation difference in how the committed conf entry is
carried. None is new; none is a safety difference; final state converges on every
deterministic scenario. The harness proved it is still sensitive — it flags these
4 in the very same run in which 5 others became MATCH.

## 0.05 Re-verification on master `430b050` (0.4.1 — reject_index, snapshot-via-Ready, canVote, singleton read gate)

The `difftest/` subtree was merged forward onto **master `430b050`** (`git merge
--no-ff master`). The merge had **zero consensus-code conflicts** — the branch is
master's consensus source plus the additive `difftest/` overlay — so every core
file (`core/*`, `tracker/*`, `raftpb/*`, `log/*`, `quorum/*`, `storage/*`,
`confchange/*`, `raft.mbt`) took master verbatim. Gate after merge:
**`moon check --deny-warn --target all` clean on all four backends, `moon test`
723 / 723**. The etcd submodule stays pinned at `26647d5` with only the untracked
`harness_export.go` overlay.

`430b050` brings the four fixes this pass targets: **reject_index in the append
reply (#22)**, **installed snapshots delivered through `Ready`**, **pre-vote
authorization aligned to etcd `canVote` (#23)**, and the **voters-only singleton
read gate (#24)**.

**Schema alignment (harness-only, no consensus change).** #22 makes
`reject_index` a live wire field: on a rejected `AppendEntries` the follower now
echoes the rejected probe point, exactly etcd's `MsgAppResp.Index` on a rejection
(`Index: m.GetIndex()`). etcd's `Index` field is *unified* — the new match index
on success, the rejected probe point on a rejection — and the Go harness already
emits it (`m.GetIndex()`). The MoonBit harness, however, still read only
`match_index` (0 on a rejection), so the two sides reported the *same* rejection
with different `index` (Go 257, mb 0). This was a **harness under-read, not a
consensus difference**: the fix is one line in `mbt-harness/main.mbt` —
`index = if a.success { a.match_index } else { a.reject_index }` — so both sides
now emit the same etcd-native `Index`. No assertion was weakened; a field the
MoonBit side was dropping is now compared.

**Result: 10 scenarios MATCH, 4 diverge — the prior MATCH set (01/02/04/05/08/10/
11/12) plus the two new positive controls (13/14). No scenario regressed from
MATCH to DIVERGE.** The DIVERGE set is byte-for-byte the same four standing causes
as `ac443c5` (§0.2): 03 (heartbeat-resp Map order), 06 (256 `msg.commit` ordering
artifacts), 07 / 09 (conf-change append shape). Scenario 06 *improved*: the
reject-`Index` diff at step 308 that `ac443c5` newly unmasked is now **gone** (257
→ 256 field diffs), leaving only the pre-existing step-305 ordering artifact.

| # | Scenario | `ac443c5` | `430b050` | Change |
|---|----------|-----------|-----------|--------|
| 01 | elect + commit | MATCH | **MATCH** | — |
| 02 | pre-vote on | MATCH | **MATCH** | — |
| 03 | partition → heal | DIVERGE | **DIVERGE** | unchanged (heartbeat-resp Map order) |
| 04 | leader crash | MATCH | **MATCH** | — |
| 05 | divergent-log truncation | MATCH | **MATCH** | — |
| 06 | flow control / batching | DIVERGE (257) | **DIVERGE (256)** | **reject-`Index` step-308 diff eliminated** by harness alignment; only step-305 ordering artifact remains |
| 07 | add voter | DIVERGE ×1 | **DIVERGE ×1** | unchanged (conf-change append shape) |
| 08 | remove voter | MATCH | **MATCH** | — |
| 09 | add learner | DIVERGE ×1 | **DIVERGE ×1** | unchanged (conf-change append shape) |
| 10 | ReadIndex (safe) | MATCH | **MATCH** | — |
| 11 | leadership transfer | MATCH | **MATCH** | — |
| 12 | repeated campaigns / stale | MATCH | **MATCH** | — |
| 13 | **reordered stale reject** (new) | — | **MATCH** | **new positive control for #22** (see below) |
| 14 | **singleton read gate** (new) | — | **MATCH** | **new coverage for #24** (see below) |

**Positive control for #22 — reproduced (previously an open gap).** The prior
report (§5 control #1, §7) could not surface the reject-`Index` staleness guard
because the network model delivers in-order per pair, so a reordered/duplicate
stale reject could not be constructed. This pass adds a **`dup from to` DSL
primitive** (symmetric on both harnesses: it re-queues a copy of every in-flight
`from→to` message so a later `deliver` steps the stale duplicate *after* the
original moved the follower's progress) and **scenario 13**. The scenario drives a
lagging follower whose `next` was advanced optimistically, so the leader probes at
`idx4`, the follower rejects (`reject_index=4, hint=2`), the reject is `dup`-ed,
and both copies are delivered together. The leader backs off once to probe `idx2`;
the stale duplicate (`reject_index=4`) is discarded by `maybe_decr_to`'s guard
(`next_index-1 = 2 ≠ 4`), emitting a single `Append 1→3 idx2` — **frame-identical
to etcd**. The control is genuine: temporarily reverting the one-line #22 leader
change (`let rejected = reply.reject_index` → `p.next_index - 1`) makes the guard
dead again, and MoonBit emits a **second spurious `Append 1→3 idx2` probe** at
step 16 → **DIVERGE (7 field diffs)**; restoring the fix returns it to MATCH. This
is the "old DIVERGE → fixed MATCH" evidence the reject_index fix required.

**Singleton read gate (#24) — covered by scenario 14.** Scenario 10 is a 3-node
cluster and never takes the `IsSingleton` fast path. Scenario 14 is a one-voter
cluster: `read_index` at step 6 emits **zero messages** on both sides — the lone
voter confirms its own leadership trivially and answers against the commit index
with no heartbeat round. MATCH. (This covers the singleton fast path; the
voters-only-*with-learner* nuance of #24 — a lone voter plus a learner still
qualifying as singleton — is not separately distinguished here and remains
exercised by the library's ported unit tests.)

**Snapshot-via-Ready and same-term `canVote` — coverage notes (honest gaps).**
The snapshot-through-`Ready` fix is **not trace-reachable**: as §5 control #2 / §7
note, the RawNode harness has no leader-side log-compaction hook, so no `MsgSnap`
is ever emitted and no scenario installs a snapshot; the fix is confirmed
structurally (723 tests, ported `TestSlowNodeRestore`/`TestLeaderTransferAfterSnapshot`)
but cannot be surfaced by the present scripts. The `canVote` #23 fix grants
pre-votes at the *same* term (voted-for-candidate or no-vote-and-no-leader
clauses); scenario 02's fresh-cluster pre-vote passes via the *future-term* clause
(a pre-candidate solicits `current+1 > voters' term`), which the pre-fix code also
satisfied, so 02 does not distinctively exercise the same-term nuance. Building a
same-term pre-vote in the DSL requires a voter parked at a higher term with no
vote and no leader — a fragile multi-step construction — so the same-term `canVote`
path is left to the library's ported `TestRecvMsgPreVote`/`TestPreVoteMigration`
(723 green); flagged here as a harness gap rather than claimed as covered.

**No new consensus bug.** Every field diff in this run was already present on
`ac443c5` (03/06/07/09) or is the newly-*aligned* reject_index field; the only
change made to make 06 stricter was the harness under-read fix. No `FINDINGS`
entry was added (no consensus-code change on this branch); the standing
`reject_index` positive-control gap in the audit ledger is now recorded as closed.

## 0.2 Re-verification on master `ac443c5` (0.4.0 breaking changes)

The framework (its 4 commits) was rebased from `5fce34a` onto **`ac443c5`**, which
adds the two 0.4.0 breaking changes on top of a docs-only commit (`f3ea10e`, test
count corrected to 708):

- **Change A** (`95b1211`): the self-invented 256-entry `MsgApp` count cap is
  removed; `AppendEntries` is now capped by **bytes only**, matching etcd's
  `sendAppend`.
- **Change B** (`a7e0a47`): the self-invented `Payload::SnapshotResp` variant is
  deleted; a follower now answers `MsgSnap` with a plain `AppendResp`, matching
  etcd's `handleSnapshot` (raft.go:1840).

Main-library gate after `moon clean`: **708 / 708 tests**, `moon check
--deny-warn --target all` clean on all four backends. The Go side stays pinned at
`26647d5` with the untracked `harness_export.go` overlay.

**MoonBit harness edit (one, harness-only, no main-library change).** The mbt
harness matched on `Payload::SnapshotResp`, deleted by change B, so it no longer
compiled. The dead match arm was removed (`difftest/mbt-harness/main.mbt`); the
harness now takes the `AppendResp` path a follower actually emits. No main-library
source was touched. The Go harness needed no change (its `"SnapshotResp"` string
case is simply never hit).

**Result: 8 scenarios MATCH, 4 diverge — the identical MATCH set as §0.1
(01/02/04/05/08/10/11/12). No scenario regressed from MATCH to DIVERGE.** The
0.4.0 changes introduced **zero regressions**.

| # | Scenario | `5fce34a` | `ac443c5` | Change | Nature of any residual diff |
|---|----------|-----------|-----------|--------|-----------------------------|
| 01 | elect + commit | MATCH | **MATCH** | — | — |
| 02 | pre-vote on | MATCH | **MATCH** | — | — |
| 03 | partition → heal | DIVERGE | **DIVERGE** | unchanged | heartbeat-resp Map-iteration order; state converges. |
| 04 | leader crash | MATCH | **MATCH** | — | — |
| 05 | divergent-log truncation | MATCH | **MATCH** | — | — |
| 06 | flow control / batching | DIVERGE | **DIVERGE (cause changed)** | **256-cap eliminated** | see below. |
| 07 | add voter | DIVERGE ×1 | **DIVERGE ×1** | unchanged | conf-change append shape (etcd 1-entry append at prev-index 1 vs mb empty append at index 2). |
| 08 | remove voter | MATCH | **MATCH** | — | — |
| 09 | add learner | DIVERGE ×1 | **DIVERGE ×1** | unchanged | same as 07. |
| 10 | ReadIndex (safe) | MATCH | **MATCH** | — | — |
| 11 | leadership transfer | MATCH | **MATCH** | — | — |
| 12 | repeated campaigns / stale | MATCH | **MATCH** | — | — |

**Change A — verified by trace (the 256-cap DIVERGENT is eliminated).** At §0.1
the two harnesses emitted **different message counts** at scenario 06 (etcd 300 in
one append, mb 256 + a 44-entry follow-up), the length mismatch cascading into the
`.commit` tail. After change A the two traces are **frame-for-frame equal in count
(308 / 308)** and **message-for-message equal in multiset** at the critical step:
both emit exactly **257 `Append` + 513 `AppendResp` = 770 messages** to the same
recipients at the same indices, and the sorted set of `(Append)` `commit` values is
**byte-identical** between the two sides. The prior 256-vs-300 count difference is
**gone**. The trace is the evidence: the self-invented cap no longer shapes the
output.

What *remains* at 06 resolves to two causes, **neither of which is the 256-cap**:

1. **Message-ordering artifact (256 field diffs, step 305).** The 257 `Append`
   messages carry the same *multiset* of `commit` values but in a different
   sequence — the interleave of `AppendResp` processing vs `Append` emission
   differs. This is the **same class as the §4 heartbeat Map-iteration order**
   (03): identical set, permuted order, identical converged state.
2. **Reject-response `Index` field (1 field diff, step 308) — newly *unmasked*,
   not newly *introduced*.** On a rejected `AppendEntries`, etcd echoes
   `Index: m.Index` (the rejected probe's `prev_log_index`, here 257);
   raft-moonbit's `handle_append_entries` reject path (`core/replication.mbt`,
   `match_index: 0`) reports 0. Both are rejects with `RejectHint = 1`; both
   followers still converge to `commit = 301`. This lives in code **untouched by
   0.4.0** — §0.1 could not see it because the 06 traces were length-mismatched and
   never aligned to step 308. Change A aligning the counts is what exposed it. It
   is a genuine (minor) port-faithfulness gap in the reject `Index` field, flagged
   here for triage; it is **not** a 0.4.0 regression.

**Change B — structurally in effect, not trace-exercised.** As §0.1/§5 note, the
RawNode harness has no leader-side log-compaction hook, so no `MsgSnap` is ever
emitted and no snapshot-reply scenario exists among the 12 scripts. Change B is
confirmed structurally — the `SnapshotResp` variant is gone (the harness stopped
compiling until its match arm was removed) and the follower now answers via
`AppendResp` (`core/raftnode_step.mbt`) — but it cannot be surfaced by trace under
the current harness. No scenario relies on the deleted variant, and none regressed.

**Bottom line.** Change A makes scenario 06 **strictly closer to etcd** (message
counts and multisets now coincide where they previously diverged) and eliminates
the 256-cap DIVERGENT. Change B is a faithful, regression-free convergence not
reachable by the present scripts. Residual DIVERGEs are the two standing §4 causes
— heartbeat Map-iteration order (03, 06-step-305) and conf-change append shape
(07, 09) — plus one newly-*unmasked* pre-existing reject-`Index` gap at 06-step-308.
The 256-cap cause is retired.

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
