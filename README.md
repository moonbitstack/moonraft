# raft-moonbit

Raft consensus algorithm implemented in MoonBit.

Raft keeps a cluster of nodes agreeing on the order of a command log even when some nodes crash or the network drops, delays and reorders messages. It is the foundation of the replicated state machines used by systems such as etcd, TiKV and Consul. This is an independent MoonBit implementation whose engineering structure references the Go [etcd-io/raft](https://github.com/etcd-io/raft).

## Features

- **Leader election** with the Follower / Candidate / Leader roles and the up-to-date-log voting restriction (§5.2, §5.4.1).
- **Log replication** through AppendEntries: the log-matching consistency check, conflicting-suffix truncation and majority commit advancement (§5.3).
- **Safety**: a candidate wins only with an up-to-date log, and committed entries are never overwritten.
- **Pluggable `StateMachine`**: committed entries are applied, in order, to any state machine you supply.
- A synchronous, deterministic cluster driver (`run_election`, `replicate`) that composes the RPC handlers into whole rounds, which keeps tests readable and makes the core easy to embed.

## Install

```
moon add heke1228/raft-moonbit
```

## Example

```moonbit
// moon.pkg.json: import { "heke1228/raft-moonbit" as raft }

// A three-node cluster.
let a = @raft.Node::new("a")
let b = @raft.Node::new("b")
let c = @raft.Node::new("c")

// Elect a leader, then replicate two commands to a majority.
let _ = @raft.run_election(a, [b, c])
let _ = a.append(a.current_term(), b"set x = 1")
let _ = a.append(a.current_term(), b"set y = 2")
let _ = @raft.replicate(a, [b, c])

// Feed the committed log into your own state machine.
a.apply_committed(my_state_machine)
```

## Architecture

| File | Responsibility |
| --- | --- |
| `state.mbt` | `Node` state and the role transitions |
| `log.mbt` | Log operations: append, last index / term, term lookup |
| `rpc.mbt` | RequestVote / AppendEntries message types |
| `election.mbt` | The RequestVote handler and the up-to-date-log rule |
| `replication.mbt` | The AppendEntries handler: matching, truncation, commit |
| `leader.mbt` | Leader-side replication with nextIndex backoff |
| `cluster.mbt` | The synchronous election driver |
| `statemachine.mbt` | The `StateMachine` trait and entry application |
| `storage.mbt` | The `LogStore` trait and crash recovery |
| `transport.mbt` | The `Transport` trait for RPC delivery |
| `membership.mbt` | Cluster membership and quorum |
| `snapshot.mbt` | Log compaction and snapshot install |

## Roadmap

- [x] Core types: term, index, role, log entry
- [x] Leader election
- [x] Log replication and commit advancement
- [x] Safety: up-to-date-log restriction and log matching
- [x] Pluggable state machine
- [x] Durable persistence behind a `LogStore` interface
- [x] Snapshot and log compaction
- [x] One-server-at-a-time membership change
- [x] Pluggable `Transport` for RPC delivery
- [ ] Deterministic simulation harness: partition, loss, reorder, crash

## License

MIT. See [LICENSE](LICENSE).

## Acknowledgement

The engineering structure references the Go implementation [etcd-io/raft](https://github.com/etcd-io/raft) (Apache-2.0). This is an independent MoonBit implementation; the types, interfaces and tests are original.
