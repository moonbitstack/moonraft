---
name: moonraft
description: "Replicated-log consensus for MoonBit. Use it when a service needs leader election, a totally ordered command log agreed by a majority, runtime membership changes, snapshots, or linearizable reads - and when you want to drive that log with your own transport, clock and storage."
---

# moonraft

Raft consensus as a library: the protocol, the persistent log and the
configuration state machine, with no runtime of its own. You supply the
transport, the disk and the state machine; it supplies the ordering.

```moonbit
import {
  "moonbitstack/moonraft" @raft,
}
```

`moon add moonbitstack/moonraft`, then import as above. The root package is a
façade — every type is written `@raft.X` no matter which sub-package owns it.

## Run a whole cluster in one process

`Cluster` is a deterministic simulator over the real consensus core: one seed,
discrete time, and a network that can drop, delay and reorder messages. Use it
for tests, for reproducing a failure, and for learning the protocol.

```moonbit
let cluster = @raft.Cluster::new(["a", "b", "c"], seed=7)
assert_eq(cluster.run_until_leader(200).unwrap(), "c")
assert_true(cluster.propose(b"set x = 1"))
assert_true(cluster.run_until_committed(2, 200))
assert_true(cluster.committed_agrees())

cluster.crash("c")
assert_true(cluster.run_until_leader(400) is Some(_))
assert_true(cluster.one_leader_per_term())
```

`one_leader_per_term` and `committed_agrees` are re-checked against the whole
run, so assert them after every failure you inject.

## Drive one server yourself

`RawNode` is the same core for a deployment: you own the clock, the disk and the
wire. `tick` advances time, `ready` hands over a batch of work, `store` persists
it, `advance` acknowledges.

```moonbit
// One voter: it elects itself, so this is also the cheapest fixture for a test.
let node = @raft.RaftNode::new("a", [])
let raw = node.raw()
let drain = fn() {
  while raw.has_ready() {
    let ready = raw.ready()
    raw.store(ready)
    for message in ready.messages {
      // hand this to your transport; step the reply back in with raw.step(m)
      ignore(message)
    }
    raw.advance(ready)
  }
}
raw.campaign()
drain()
assert_true(node.is_leader())
```

With peers, `campaign` needs their votes: keep the messages, deliver them, and
`step` each reply back in — the same loop, with the transport in the middle.

## Turn the log into state

`StateMachine` is the only contract between consensus and your application:
`apply` runs once per committed entry, in log order, on every replica.

```moonbit
struct Kv {
  data : Map[String, String]
}

impl @raft.StateMachine for Kv with fn apply(self, command) {
  let s = command.to_unchecked_string()
  let mut key = ""
  let mut value = ""
  let mut seen_eq = false
  for c in s {
    if !seen_eq && c == '=' {
      seen_eq = true
    } else if seen_eq {
      value = value + "\{c}"
    } else {
      key = key + "\{c}"
    }
  }
  self.data[key] = value
}
```

```moonbit
let kv : Kv = { data: Map([]) }
@raft.StateMachine::apply(kv, b"service=moonraft")
assert_eq(kv.data.get("service"), Some("moonraft"))
```

## Rules that catch people out

- **Propose, never mutate.** Membership, snapshots and reads all go through the
  log or through `RawNode` methods. Writing a configuration by hand desynchronises
  the quorum arithmetic from what the other servers believe.
- **Persist before you acknowledge.** `store` writes the entries and the hard
  state; `advance` says they are durable. Acknowledging first loses the entry on
  the next crash.
- **`ready` only when `has_ready`.** Taking a `Ready` with no work returns an
  empty batch, and `advance` on it is a no-op rather than a reset.
- **A leader alone is not a majority.** Proposals are queued until a quorum
  acknowledges; with the cluster split, `propose` can return true and never
  commit.
- **Logging is silent by default.** The core logs through a one-method `Logger`
  trait; pass one to `RaftNode::new` (`logger=`) or you get nothing, which is
  what a deterministic simulation wants.
- **Errors carry the reason.** Storage reports `Compacted`, `Unavailable` and
  `SnapOutOfDate` separately, because "send a snapshot" and "wait" need
  opposite responses.
