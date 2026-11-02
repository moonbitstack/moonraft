# Porting notes: etcd-io/raft reconciliation

This library is a MoonBit port of [`etcd-io/raft`](https://github.com/etcd-io/raft)
pinned at commit `26647d5`. This page records how the upstream *datadriven*
test corpus is replayed, and the one deliberate divergence from upstream.

## Datadriven replay

Upstream ships **41** `datadriven` scripts (the `testdata/*.txt` corpus):
28 under `raft/testdata/`, 9 under `confchange/testdata/`, and 4 under
`quorum/testdata/`. They are archived **verbatim** under `testdata/etcd/` and
embedded (see `tools/gen_dd_scripts.py`) into `src/core/dd_scripts_wbtest.mbt`
so they can be replayed in CI without file IO.

`src/core/datadriven_wbtest.mbt` is an in-repo interpreter for the
`cockroachdb/datadriven` format. It parses each script into directive / input /
expected blocks and drives the command sequence through the matching harness:

| Harness | Drives | Source |
| --- | --- | --- |
| `RaftTest` (`rafttest_wbtest.mbt`) | the `RawNode` Ready/Advance contract, a message queue, and the raft log | `testdata/testdata/*.txt` |
| `ConfChangeTest` (`ddatest_wbtest.mbt`) | the `Changer` (`simple` / `enter-joint` / `leave-joint`) | `testdata/confchange/testdata/*.txt` |
| `run_quorum` (`ddatest_wbtest.mbt`) | `vote_result` and `committed_index` | `testdata/quorum/testdata/*.txt` |

### Comparison against the expected blocks

The harness executes every command upstream understands and renders its output in
upstream's own format (`src/core/rafttest_format_wbtest.mbt` is a port of
`util.go`'s `DescribeReady` / `DescribeMessage` / `DescribeEntry` family), so a
script's expected block is a byte comparison rather than a shape check.

- The 13 `confchange` and `quorum` scripts compare case by case and match
  exactly (`datadriven: quorum and confchange scripts reproduce upstream exactly`).
- The 28 `raft` scripts are scored block by block
  (`datadriven: raft scripts reproduce upstream block by block`): **477 of 558**
  cases match on the `core-script-parity` branch. What closed since the first
  score is mostly ordering, not wording:

  - every vote and append acknowledgement waits for the state it attests to be
    durable (upstream's `msgsAfterAppend` → `stepsOnAdvance`), the storage
    threads' replies travel as messages (`MsgStorageAppendResp` /
    `MsgStorageApplyResp`), and a leader counts its own append only once it is
    durable;
  - a committed configuration change takes effect when it is *applied*, and the
    leader then announces the new configuration and probes the servers it added
    (etcd's `applyConfChange` → `switchToConfig`), so `switched to configuration`
    lands in the block where upstream prints it;
  - a newly added server starts optimistically at the leader's last index, and
    the snapshot a leader ships is the state its storage holds — the applied
    index — rather than its compaction point;
  - the append, rejection, snapshot-send, snapshot-restore, leadership-transfer
    and auto-leave paths now emit upstream's diagnostics.

  The remaining 81 cases are concentrated in the two async-write scripts (their
  message ordering once several storage directives are in flight: 21/44 and
  34/47), the two snapshot scripts (snapshot timing, 17/18 and 20/25), and a
  handful of per-script differences (message grouping in `lagging_commit`, the
  probe ladder in `heartbeat_resp_recovers_from_probing`, `paused` bookkeeping
  in `replicate_pause`).

The scoreboard test fails if the count drops, so a change that stops a script
reproducing upstream is caught rather than absorbed.


### `raft/testdata (28)`

- `testdata/etcd/testdata/async_storage_writes.txt`
- `testdata/etcd/testdata/async_storage_writes_append_aba_race.txt`
- `testdata/etcd/testdata/campaign.txt`
- `testdata/etcd/testdata/campaign_learner_must_vote.txt`
- `testdata/etcd/testdata/checkquorum.txt`
- `testdata/etcd/testdata/confchange_disable_validation.txt`
- `testdata/etcd/testdata/confchange_v1_add_single.txt`
- `testdata/etcd/testdata/confchange_v1_remove_leader.txt`
- `testdata/etcd/testdata/confchange_v1_remove_leader_stepdown.txt`
- `testdata/etcd/testdata/confchange_v2_add_double_auto.txt`
- `testdata/etcd/testdata/confchange_v2_add_double_implicit.txt`
- `testdata/etcd/testdata/confchange_v2_add_single_auto.txt`
- `testdata/etcd/testdata/confchange_v2_add_single_explicit.txt`
- `testdata/etcd/testdata/confchange_v2_replace_leader.txt`
- `testdata/etcd/testdata/confchange_v2_replace_leader_stepdown.txt`
- `testdata/etcd/testdata/forget_leader.txt`
- `testdata/etcd/testdata/forget_leader_prevote_checkquorum.txt`
- `testdata/etcd/testdata/forget_leader_read_only_lease_based.txt`
- `testdata/etcd/testdata/heartbeat_resp_recovers_from_probing.txt`
- `testdata/etcd/testdata/lagging_commit.txt`
- `testdata/etcd/testdata/prevote.txt`
- `testdata/etcd/testdata/prevote_checkquorum.txt`
- `testdata/etcd/testdata/probe_and_replicate.txt`
- `testdata/etcd/testdata/replicate_pause.txt`
- `testdata/etcd/testdata/single_node.txt`
- `testdata/etcd/testdata/slow_follower_after_compaction.txt`
- `testdata/etcd/testdata/snapshot_succeed_via_app_resp.txt`
- `testdata/etcd/testdata/snapshot_succeed_via_app_resp_behind.txt`

### `confchange/testdata (9)`

- `testdata/etcd/confchange/testdata/joint_autoleave.txt`
- `testdata/etcd/confchange/testdata/joint_idempotency.txt`
- `testdata/etcd/confchange/testdata/joint_learners_next.txt`
- `testdata/etcd/confchange/testdata/joint_safety.txt`
- `testdata/etcd/confchange/testdata/simple_idempotency.txt`
- `testdata/etcd/confchange/testdata/simple_promote_demote.txt`
- `testdata/etcd/confchange/testdata/simple_safety.txt`
- `testdata/etcd/confchange/testdata/update.txt`
- `testdata/etcd/confchange/testdata/zero.txt`

### `quorum/testdata (4)`

- `testdata/etcd/quorum/testdata/joint_commit.txt`
- `testdata/etcd/quorum/testdata/joint_vote.txt`
- `testdata/etcd/quorum/testdata/majority_commit.txt`
- `testdata/etcd/quorum/testdata/majority_vote.txt`

