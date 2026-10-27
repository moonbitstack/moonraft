<div align="center">

# moonraft

**A production-grade Raft consensus library in MoonBit.**

[![mooncakes](https://img.shields.io/badge/mooncakes-moonbitstack%2Fmoonraft-26b483)](https://mooncakes.io/docs/moonbitstack/moonraft)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**[▶ Live demo](https://moonbitstack.github.io/moonraft/demo.html)** · **[Docs](https://moonbitstack.github.io/moonraft/)** · **[API](https://moonbitstack.github.io/moonraft/api.html)** · **[Quickstart](https://moonbitstack.github.io/moonraft/quickstart.html)**

</div>

Raft keeps a cluster of nodes agreeing on the order of a command log even when nodes crash and the network drops, delays and reorders messages — the foundation of the replicated state machines behind distributed stores, schedulers and coordination services. This library implements the full protocol — election, replication, membership, persistence and reads — as a self-contained MoonBit package you drive from one of two fronts.

It ships two ways to drive one consensus core:

- a **synchronous driver** (`run_election`, `replicate`) that composes the RPC handlers into whole rounds — small and easy to read or embed; and
- a **message-driven server** (`RaftNode`) that speaks only in `Message`s through `tick` and `step`, so a real transport — or the bundled deterministic simulator — can drive it.

## Install

```
moon add moonbitstack/moonraft
```

## Quick example

```moonbit
// Drive a five-node cluster through the deterministic simulator.
let cluster = @raft.Cluster::new(["a", "b", "c", "d", "e"], seed=1)
let leader = cluster.run_until_leader(200)          // elect a leader
let _ = cluster.propose(b"set x = 1")               // replicate a command
let _ = cluster.run_until_committed(2, 200)         // wait for commit

cluster.crash(leader.unwrap())                       // inject a fault
let _ = cluster.run_until_leader(400)               // a new leader takes over
assert_true(cluster.one_leader_per_term())          // safety still holds
assert_true(cluster.committed_agrees())
```

Or run the bundled example — a five-node cluster elects, replicates, loses its leader and re-elects, printing the safety invariants at each step:

```
git clone https://github.com/moonbitstack/moonraft && cd moonraft
moon run src/cmd/example
```

<details><summary>Deterministic output (same seed → same transcript)</summary>

```
cluster of 5 nodes, seed 1
elected leader: c
committed 'set x = 1' on a majority
crashed the leader
new leader: a
one leader per term : true
committed prefixes agree : true
safety invariants hold : true
```

Source: [`src/cmd/example/main.mbt`](src/cmd/example/main.mbt). The lower-level `Node` / `RaftNode` APIs are used directly in the tests — see `raftnode_wbtest.mbt` (message-driven) and `cluster_wbtest.mbt` (synchronous).

</details>

New to the library? [`src/examples/`](src/examples/) is a guided tour of short, runnable programs — each one showcasing a different feature, from a first election up to a replicated key-value store and the lower-level `RawNode` deployment loop. They are meant to overlap on purpose: some show the same idea through the high-level `Cluster` simulator, others through the message-driven `RawNode`, so you can pick the layer you care about.

```
# The high-level, deterministic Cluster simulator — the easiest place to start
moon run src/examples/00-helloworld      # a minimal system: elect a leader
moon run src/examples/01-replicate       # propose commands, confirm every node commits
moon run src/examples/02-fault-tolerance # crash, partition, drop packets — safety holds
moon run src/examples/03-operations      # progress, leadership transfer, compaction
moon run src/examples/04-kvstore         # a complete replicated key-value store on a StateMachine

# Membership and the two roles a node can play
moon run src/examples/05-membership      # add and remove voters (ConfChange) at runtime
moon run src/examples/06-learner         # a non-voting learner, then promote it to a voter

# Fault handling and log management
moon run src/examples/07-checkquorum     # check-quorum: a leader steps down when it loses its majority
moon run src/examples/08-snapshot        # compact the log; a restarted node catches up from a snapshot

# The message-driven RawNode — the API you use to deploy Raft on a real transport
moon run src/examples/09-rawnode         # the RawNode tick → Ready → Advance loop, by hand
moon run src/examples/10-storage         # persist Raft state to MemoryStorage and recover it
moon run src/examples/11-readindex       # a linearizable (ReadOnlySafe) read, confirmed by quorum
```

The `Cluster` simulator ([`src/examples/00`–`08`](src/examples/)) gives you a seeded, deterministic cluster in a single process. `RawNode` ([`src/examples/09`–`11`](src/examples/)) is the same consensus core driven by your own ticks and messages — that is the layer you wire to a network and a real storage backend.

## How it stays correct

Raft's value is its safety properties, and the library checks them as it runs rather than only on paper. The deterministic simulator (`Cluster`) re-checks, on every step, that there is at most one leader per term, that every committed prefix agrees across the cluster, and that the logs stay consistent — the same three invariants the in-browser demo re-checks on every frame. Each feature below is exercised by a runnable example, and every public type is documented in the [API reference](https://moonbitstack.github.io/moonraft/api.html).

## Live demo — real consensus in your browser

### ▶ https://moonbitstack.github.io/moonraft/demo.html

Five nodes, five Web Workers. Each worker instantiates its own copy of this consensus core compiled to **WebAssembly**, ticks on its own wall-clock timer, and talks to peers only by `postMessage`. The main thread is the network — drop packets, add delay, **split** the cluster, **isolate** or **crash** the leader — and it holds no Raft state of its own. Elections race, messages reorder, nothing about the schedule is deterministic; a panel re-checks the safety invariants (*one leader per term*, *committed prefixes agree*) on every frame.

Click **Split 2 | 3** and you can watch two nodes lead *different* terms at once - and **Election Safety still holds**, because two leaders only contradict Raft if they share a term, and the stale one cannot reach a majority, so it cannot commit. Heal the partition and it steps down.

It is not a JavaScript re-implementation — messages cross the boundary as flat integers, node state is a JSON string read straight out of the wasm module's linear memory, and every transition happens inside the same MoonBit code the tests exercise (`src/examples/browser`). Five workers on one machine model *concurrency*, not a distributed deployment, and a restarted node catches up from the leader since the workers have no persistent storage.

<details><summary>Build and run the site locally</summary>

```
moon build --target wasm --release src/examples/browser
cp _build/wasm/release/build/examples/browser/browser.wasm docs/moonraft.wasm
python3 -m http.server 8099 --directory docs    # then open http://localhost:8099/
```

Workers `fetch` the wasm, so a `file://` URL will not work.

</details>

## Features

<details open><summary><b>A complete Raft, not a sketch</b> — click to collapse</summary>

- **Leader election** with the Follower / Candidate / Leader roles and the up-to-date-log voting restriction (§5.2, §5.4.1).
- **Pre-vote** so a partitioned node cannot inflate the cluster term, plus randomized election timeouts and heartbeats off a per-node deterministic PRNG.
- **Log replication** through AppendEntries: log-matching, conflicting-suffix truncation, and majority commit within the current term (§5.3, §5.4.2). Replies carry a `conflict_index` hint for one-jump backoff and a `reject_index` that keeps a reordered rejection from driving a spurious back-off; per-follower `Progress` (probe / replicate / snapshot) drives repair, including from heartbeat acks.
- **Snapshots and log compaction** (§7): `compact`, the `InstallSnapshot` RPC, and automatic snapshot fallback for a follower whose next entry was already compacted away.
- **Membership changes** (§4, §6): single-server add/remove and full **joint consensus** with `ConfChangeV2` and auto-leave — C(old,new) needs a majority of *both* halves, and the leader appends the leave entry itself once it commits. A committed change reconfigures the running node: quorums resize, a leader that removed itself steps down, and an in-flight transfer to a removed target aborts.
- **Learners** (§4.2.1): non-voting members that receive the log, never campaign and never count toward a quorum, with promotion to voter and `learners_next`-staged demotion across a joint change.
- **Flow control**: a sliding-window `Inflights` limit, a byte cap per batch (`MaxSizePerMsg`), and a bound on the uncommitted tail (`MaxUncommittedEntriesSize`).
- **`raftLog` split into stable storage and an `unstable` tail** with in-progress bookkeeping and byte-level pagination, so a caller knows exactly what to persist and what to apply.
- **`RawNode` with `Ready` / `Advance`**: ask whether there is work, take a batch (entries to persist, `HardState`/`SoftState` if changed, messages, committed entries, read states), do it, acknowledge — no threads, no async.
- **Persistence and crash recovery** (§5.3): a `HardState`, an append-only write-ahead log (`WalStore`) with replay, and a `MemoryStorage` engine. Storage reads report `Compacted`, `Unavailable` and `SnapOutOfDate` as distinct errors, so a caller can tell "send a snapshot" from "wait".
- **Linearizable reads** in both modes — `ReadOnlySafe` (fresh quorum round-trip) and lease-based — plus **check-quorum**, which steps a leader down when it loses a majority and makes followers refuse disruptive votes.
- **Leadership transfer** (`TimeoutNow`, §3.10): the target is caught up first, proposals are blocked mid-transfer, and it aborts on timeout, step-down or removal.
- **Pluggable `StateMachine`, `Transport`, `LogStore` and `RaftStorage`** traits, with a replicated key-value store as the worked example.
- **Deterministic simulation harness** (`Cluster`): a single-seed discrete-time network that drops, delays, reorders, partitions and crashes/restarts nodes, with built-in safety-invariant checks and a suite of scenario and chaos tests.

</details>

## Architecture

The public surface is a pure facade: the root package re-exports the symbols so consumers write `@raft.X` regardless of where a type lives. The implementation is split into focused packages, each owning one concern:

| Package | Responsibility |
| --- | --- |
| `quorum/` | Majority and joint-configuration vote counting |
| `tracker/` | Per-follower `Progress` (Probe / Replicate / Snapshot) and the `Inflights` window |
| `raftpb/` | On-the-wire types: `Entry`, `Message`, RPCs, `HardState`, `Snapshot`, `ConfState`, entry sizing |
| `confchange/` | Configurations, joint consensus, `ConfChange` and the config `Changer` |
| `storage/` | `MemoryStorage`, the write-ahead log, and the `LogStore` / `RaftStorage` traits |
| `log/` | `RaftLog`, the unstable tail, term lookup and bounded slices |
| `core/` | The consensus engine: `RaftNode` / `Node` step & dispatch, election, replication, snapshots, ReadIndex, leader lease, check-quorum, `RawNode` / `Ready`, `Config`, and the simulator |

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
