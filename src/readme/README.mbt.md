# moonraft — a working tour

**A production-grade Raft consensus library in MoonBit.**

Every `mbt check` block on this page is part of the build: `moon test` compiles it
and runs the tests in it, so none of the code below can drift from the library.
The prose README is [README.md](../../README.md); this page is the part of it that
has to stay executable.

## Install

```
moon add moonbitstack/moonraft
```

The public surface is one façade package, so a consumer writes `@raft.X` no
matter which of the focused sub-packages a type actually lives in.

## A five-node cluster, end to end

`Cluster` is a deterministic simulator over the real consensus core: one seed,
discrete time, and a network that can drop, delay and reorder messages. That
makes a whole election and a failure scenario a handful of lines — and it is the
same `RaftNode` code a deployment runs.

```mbt check
///|
test "readme: elect, replicate, lose the leader, re-elect" {
  let cluster = @raft.Cluster::new(["a", "b", "c", "d", "e"], seed=1)

  // Elect a leader. The simulator returns `None` if no node wins inside the tick
  // budget, so a flaky configuration fails here rather than downstream.
  let leader = cluster.run_until_leader(200)
  assert_true(leader is Some(_))

  // Replicate a command and wait for a majority to commit it. `propose` reports
  // whether there was a leader to take it.
  assert_true(cluster.propose(b"set x = 1"))
  assert_true(cluster.run_until_committed(2, 200))

  // Crash the leader. The survivors must elect someone else, and the safety
  // invariants must still hold afterwards — that is the whole claim of Raft.
  cluster.crash(leader.unwrap())
  assert_true(cluster.run_until_leader(400) is Some(_))
  assert_true(cluster.one_leader_per_term())
  assert_true(cluster.committed_agrees())
}
```

The two invariants are re-checked continuously, not just at the end:
`one_leader_per_term` looks at every term the cluster has ever entered, and
`committed_agrees` compares the committed prefix across every running node.

## Membership changes at runtime

A configuration change is proposed like any other command and takes effect when
it commits. Adding a voter grows the quorum; from then on the new server's
acknowledgement is required.

```mbt check
///|
test "readme: grow the cluster with a configuration change" {
  let cluster = @raft.Cluster::new(["a", "b", "c"], seed=7)
  assert_true(cluster.run_until_leader(200) is Some(_))

  // `d` joins as a voter. The entry replicates under the *old* configuration and
  // is folded in when it commits, so the quorum only widens afterwards.
  assert_true(cluster.propose_conf(@raft.ConfChange::add("d")))
  assert_true(cluster.run_until_committed(2, 200))
  assert_true(cluster.committed_agrees())
}
```

## The same core through `RawNode`

`Cluster` is the convenient front. A deployment usually wants the `RawNode` loop
instead: `tick` to advance the clock, take a `Ready` when there is work, persist
its entries, apply its committed entries, send its messages, then `advance`. No
threads, no async, and nothing hidden.

```mbt check
///|
test "readme: drive one node through tick, Ready and Advance" {
  // A single-voter group needs no peers, so it commits as soon as its own append
  // is durable — the smallest complete Ready/Advance cycle.
  let node = @raft.RaftNode::new("a", [], pre_vote=false)
  let raw = node.raw()

  raw.campaign()
  assert_true(raw.has_ready())

  let mut saw_commit = false
  while raw.has_ready() {
    let ready = raw.ready()
    // Persist first: entries and the hard state are only safe to acknowledge
    // once they are on disk. `store` does that against the driver's storage.
    raw.store(ready)
    if ready.committed_entries.length() > 0 {
      saw_commit = true
    }
    for message in ready.messages {
      // A real deployment hands these to its transport. Here there is only one
      // voter, so there is nobody to send to.
      ignore(message)
    }
    raw.advance(ready)
  }
  // Campaigning appends the leader's no-op, so the group commits without any
  // proposal at all.
  assert_true(saw_commit)
  assert_true(raw.status().role is @raft.Role::Leader)
}
```

Proposing through the same driver is `propose` plus another turn of the loop:

```mbt check
///|
test "readme: propose a command through RawNode" {
  let node = @raft.RaftNode::new("a", [], pre_vote=false)
  let raw = node.raw()
  raw.campaign()

  let applied : Array[@raft.Entry] = []
  let run = fn() {
    while raw.has_ready() {
      let ready = raw.ready()
      raw.store(ready)
      for entry in ready.committed_entries {
        applied.push(entry)
      }
      raw.advance(ready)
    }
  }
  run()

  raw.propose(b"set x = 2")
  run()
  // The command is the last thing applied, after the leader's no-op.
  assert_true(applied.length() >= 2)
  assert_eq(applied[applied.length() - 1].command, b"set x = 2")
}
```

## Logging

The core logs through a one-method `Logger` trait and defaults to silence.
`moonlog_bridge` wires it to moonlog without the consensus engine depending on a
logging backend:

```mbt check
///|
test "readme: hand a moonlog-backed logger to the core" {
  // A hub with no sinks accepts the records and writes nothing, which is what a
  // test wants. Add a slot to route them to stderr, a file, or a collector.
  let bridge = @moonlog_bridge.MoonlogLogger::of(@moonlog.Hub::new())
  // `as_logger` is the `core.Logger` that `Config` and `RaftNode::new` take.
  let logger = bridge.as_logger()
  logger.log(@raft.LogLevel::Debug, "a line nobody asked for")
}
```

## Where to go next

- [`src/examples/`](../examples/) — twelve short runnable programs, from a first
  election up to a replicated key-value store and the `RawNode` deployment loop.
- [docs/porting.md](../../docs/porting.md) — how the port is reconciled with the
  pinned upstream, and how the datadriven corpus is replayed.
- [API reference](https://moonbitstack.github.io/moonraft/api.html).
