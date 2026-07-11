# Examples

A guided tour of the public `@raft` API, from a first election to a full
replicated key-value store. Each folder is a runnable `main` package; read them
in order or jump to the one that matches your task.

```bash
moon run examples/00-helloworld
moon run examples/01-replicate
moon run examples/02-fault-tolerance
moon run examples/03-operations
moon run examples/04-kvstore
```

| # | Example | What it teaches | Key API |
| --- | --- | --- | --- |
| 00 | [`helloworld`](00-helloworld/) | Bring up a five-node cluster and elect a leader | `Cluster::new`, `run_until_leader`, `node`, `term`, `leader` |
| 01 | [`replicate`](01-replicate/) | Propose commands and confirm every node commits the same prefix | `propose`, `run_until_committed`, `commit_index`, `committed_agrees` |
| 02 | [`fault-tolerance`](02-fault-tolerance/) | Crash, partition, isolate and drop packets - then check the safety invariants held | `crash`/`restart`, `partition`/`isolate`/`heal`, `set_drop`/`set_delay`, `one_leader_per_term`, `committed_agrees`, `logs_consistent` |
| 03 | [`operations`](03-operations/) | Inspect progress, transfer leadership, compact behind a snapshot, enable check-quorum | `progress_status`, `transfer_leadership`, `compact_leader`, `enable_check_quorum` |
| 04 | [`kvstore`](04-kvstore/) | The capstone: a replicated key-value store on top of consensus | `StateMachine` trait, `RaftNode::node`, `Node::apply_committed`, `same_committed_commands` |

Every example is deterministic (each `Cluster` is seeded), so the output is
reproducible run to run. The high-level `Cluster` is a single-process
simulator - a real deployment drives the same core through `RawNode`'s
`Ready`/`Advance` loop, which the tests exercise directly.
