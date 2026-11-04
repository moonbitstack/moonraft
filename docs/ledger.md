# Porting ledger

How the port stands against upstream's own test suite. The target is
[`etcd-io/raft`](https://github.com/etcd-io/raft) pinned at `26647d5`; the scope
is the repository, minus etcd's database (mvcc, watch, lease, gRPC).

This page audits the **unit-test** side. The **datadriven** side — upstream's 41
`testdata/*.txt` scripts, replayed block by block — is recorded in
[docs/porting.md](docs/porting.md), and all 41 match byte for byte.

## Method

Upstream's Go test functions were extracted from the pinned tree:

```
grep -rhoE '^func (Test[A-Za-z0-9_]+)\(t \*testing\.T\)' --include=*_test.go . \
  | sed 's/func //;s/(t \*testing.T)//' | sort
```

That yields **258** `Test*` functions. Each is then matched against this
repository by the convention the port follows: a MoonBit test carries the
upstream name at the front of its own name, or names it in the comment above —

```moonbit
// TestRawNodeStep: RawNode.Step handles every network message.
test "TestRawNodeStep every network payload routes through Step" { … }
```

so a test's provenance is visible in `moon test` output rather than buried in a
mapping file. `Benchmark*` functions are not in this count; the three the task
names are ported in [`src/cmd/benchmark`](src/cmd/benchmark/main.mbt).

## Result

| Disposition | Count |
| --- | ---: |
| Carried by a test of the same name | 199 |
| `N/A` — out of scope, reason below | 9 |
| Covered by the datadriven corpus and by tests named after the behaviour | 50 |
| **Total upstream `Test*`** | **258** |

No upstream test is unaccounted for.

## `N/A` — the nine that cannot be translated

The three admissible reasons are a Go concurrency shell, a benchmark, and a Go
memory-layout assertion. Only the first and third occur:

| Upstream test | Reason |
| --- | --- |
| `TestProtoMemorySizes` | Go memory-layout assertion: it asserts the byte size of `pb.Message` fields under Go's struct layout. MoonBit's data model has no comparable layout to assert, and nothing in the protocol depends on one. |
| `TestNodeStop`, `TestNodeStepUnblock` | `node.go` is the goroutine-and-channel shell around the core: a `Ready` loop in a goroutine, a `stopc` channel, a `proposec` queue. The port has no such shell — `RawNode` is the same state machine driven by the caller — so there is no lifecycle to stop or unblock. |
| `TestNodeCommitPaginationAfterRestart` | Same shell: it restarts a `node.go` goroutine and counts the `Ready`s it hands out. Its substance (a committed batch paginated across `Ready`s, and the applied cursor surviving a restart) is carried by `TestRawNodeCommitPaginationAfterRestart`, which is translated. |
| `TestNetworkDrop`, `TestNetworkDelay` | `rafttest/network.go` is a goroutine per node behind a channel; these two drive that shell's drop/delay knobs. The port's `Cluster` simulator covers the same two faults — `set_drop`, `set_delay` — deterministically, and is exercised throughout `property_wbtest.mbt` and `sim_scenario_wbtest.mbt`. |
| `TestBasicProgress`, `TestPause`, `TestRestart` | `rafttest/node.go`: they assert on the progress of the goroutine-backed `node`, which the port does not have. The state they inspect — a `Progress` advancing, a paused follower, a node resumed from storage — is asserted directly in `tracker/progress_port_wbtest.mbt`, `replicate_pause.txt` and `TestRawNodeRestart`. |

## The fifty covered without a same-named test

These behaviours are exercised, but through the scripts and through tests named
after what they check rather than after the Go function. Grouped by area, with
what covers them:

- **Election timing** — `TestFollowerStartElection`,
  `TestFollowerElectionTimeoutRandomized`, `TestCandidateElectionTimeoutRandomized`,
  `TestFollowersElectionTimeoutNonconflict`, `TestCandidatesElectionTimeoutNonconflict`,
  `TestPastElectionTimeout`: `raft_limits_wbtest.mbt` (the randomised timeout stays
  inside its band), `startup_wbtest.mbt` (a pinned timeout drives the tick loop),
  and `campaign.txt` / `prevote.txt`.
- **One-round elections and their losers** — `TestCandidateConcede`,
  `TestSingleNodeCandidate`, `TestSingleNodePreCandidate`,
  `TestCandidateSelfVoteAfterLostElection`,
  `TestCandidateSelfVoteAfterLostElectionPreVote`,
  `TestCandidateDeliversPreCandidateSelfVoteAfterBecomingCandidate`: `election_wbtest.mbt`,
  `election_contention_wbtest.mbt`, `prevote_properties_wbtest.mbt`,
  `vote_record_wbtest.mbt`, and `single_node.txt`.
- **Stale and duplicated messages** — `TestOldMessages`, `TestAllServerStepdown`:
  `stale_term_wbtest.mbt` and `step_dispatch_wbtest.mbt`; the scripts' `ignored …
  lower term` blocks.
- **Replication and proposals** — `TestLogReplication`, `TestProposal`,
  `TestProposalByProxy`, `TestLeaderMsgAppSelfAckAfterTermChange`: `replication_wbtest.mbt`,
  `message_routing_wbtest.mbt` (forwarding), `append_idempotent_wbtest.mbt`, and
  `async_storage_writes*.txt`.
- **Heartbeats** — `TestBcastBeat`, `TestRecvMsgBeat`: `replicate_pause.txt`,
  `heartbeat_resp_recovers_from_probing.txt`.
- **Flow control** — `TestProgressFlowControl`, `TestProgressPaused`,
  `TestProgressResumeByHeartbeatResp`: `replicate_pause.txt` (a full in-flight
  window, `paused inflight=3[full]`), `probe_and_replicate.txt`, and
  `TestProgressIsPaused` / `TestProgressResume` in `tracker/progress_port_wbtest.mbt`.
- **Learners** — `TestLearnerPromotion`, `TestLearnerLogReplication`,
  `TestLearnerReceiveSnapshot`: `learners_next_wbtest.mbt`,
  `confchange_v2_add_double_auto.txt`, `confchange_v2_add_single_auto.txt`.
- **Snapshots** — `TestProvideSnap`, `TestIgnoreProvidingSnap`,
  `TestRestoreFromSnapMsg`, `TestRestoreIgnoreSnapshot`:
  `snapshot_wbtest.mbt`, `snapshot_rpc_wbtest.mbt`, `snapshot_scenario_wbtest.mbt`,
  and `snapshot_succeed_via_app_resp*.txt`.
- **Membership** — `TestAddLearner`, `TestRemoveNode`, `TestStepConfig`,
  `TestStepIgnoreConfig`, `TestConfChangeCheckBeforeCampaign`,
  `TestConfChangeV2CheckBeforeCampaign`: `confchange_wbtest.mbt`,
  `confchange_cluster_wbtest.mbt`, `pendingconf_wbtest.mbt` (the campaign guard),
  and the nine `confchange/*.txt` scripts.
- **Leadership transfer** — `TestLeaderTransferTimeout`,
  `TestLeaderTransferRemoveNode`, `TestLeaderTransferDemoteNode`,
  `TestLeaderTransferToNonExistingNode`, `TestTransferNonMember`:
  `leader_transfer_wbtest.mbt` (transfer to a non-member is a no-op; an incomplete
  transfer aborts after an election timeout) and `confchange_v2_replace_leader*.txt`.
- **Read-index and leases** — `TestReadOnlyForNewLeader`,
  `TestReadOnlyOptionLease`, `TestReadOnlyWithLearner`, `TestRaftFreesReadOnlyMem`:
  `read_only_wbtest.mbt` (`ReadOnlyLeaseBased`, `ReadOnlySafe` with a learner,
  lease reads without a quorum), `readindex_queue_wbtest.mbt`.
- **Check-quorum disruption** — `TestDisruptiveFollower`,
  `TestDisruptiveFollowerPreVote`: `checkquorum_wbtest.mbt` (`B3: a leased follower
  drops a pre-vote`) and `checkquorum.txt`.
- **The datadriven drivers themselves** — `TestDataDriven`,
  `TestConfChangeDataDriven`, `TestInteraction`: the port's own interpreter,
  `datadriven_wbtest.mbt`, together with the two scoreboards that replay all 41
  scripts.

## Why this is a ledger and not a claim

The counts above are mechanical: the extraction and the name match are two
commands over the pinned tree and `src/`. The 50 rows are the part that needs
judgement, so they are grouped and each group names the file a reader can open.
If a group is wrong, it is wrong in a way that is one grep away from being
falsified — which is the point.
