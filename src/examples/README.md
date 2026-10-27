# Examples

A guided tour of the public `@raft` API, from a first election to a full
replicated key-value store and the lower-level `RawNode` deployment loop. Each
folder is a runnable `main` package; the examples overlap on purpose - some
show the same idea through the deterministic `Cluster` simulator, others
through the message-driven `RawNode` - so you can pick the layer you care
about. They are meant to be read in order, but each stands alone.

```bash
# The high-level Cluster simulator - the easiest place to start
moon run src/examples/00-helloworld      # elect a leader
moon run src/examples/01-replicate       # propose commands, confirm every node commits
moon run src/examples/02-fault-tolerance # crash, partition, drop packets - safety holds
moon run src/examples/03-operations      # progress, leadership transfer, compaction
moon run src/examples/04-kvstore         # a complete replicated key-value store

# Membership and the two roles a node can play
moon run src/examples/05-membership      # add and remove voters at runtime
moon run src/examples/06-learner         # a non-voting learner, then promote it to a voter

# Fault handling and log management
moon run src/examples/07-checkquorum     # a leader steps down when it loses its majority
moon run src/examples/08-snapshot        # compact the log; a restarted node catches up

# The message-driven RawNode - the API you use to deploy Raft on a real transport
moon run src/examples/09-rawnode         # the RawNode tick -> Ready -> Advance loop, by hand
moon run src/examples/10-storage         # persist Raft state to MemoryStorage and recover it
moon run src/examples/11-readindex       # a linearizable (ReadOnlySafe) read, confirmed by quorum
```

| # | Example | What it teaches | Key API |
| --- | --- | --- | --- |
| 00 | [`helloworld`](00-helloworld/) | Bring up a five-node cluster and elect a leader | `Cluster::new`, `run_until_leader`, `node`, `term`, `leader` |
| 01 | [`replicate`](01-replicate/) | Propose commands and confirm every node commits the same prefix | `propose`, `run_until_committed`, `commit_index`, `committed_agrees` |
| 02 | [`fault-tolerance`](02-fault-tolerance/) | Crash, partition, isolate and drop packets - then check the safety invariants held | `crash`/`restart`, `partition`/`isolate`/`heal`, `set_drop`/`set_delay`, `one_leader_per_term`, `committed_agrees`, `logs_consistent` |
| 03 | [`operations`](03-operations/) | Inspect progress, transfer leadership, compact behind a snapshot, enable check-quorum | `progress_status`, `transfer_leadership`, `compact_leader`, `enable_check_quorum` |
| 04 | [`kvstore`](04-kvstore/) | The capstone: a replicated key-value store on top of consensus | `StateMachine` trait, `RaftNode::node`, `Node::apply_committed`, `same_committed_commands` |
| 05 | [`membership`](05-membership/) | Change the voter set at runtime: add a node, then remove another | `ConfChange::add`/`remove`, `propose_conf`, `conf_state`, `voter_nodes` |
| 06 | [`learner`](06-learner/) | A non-voting learner that follows but never wins an election, then promote it | `ConfChange::add_learner`, `conf_state().learners`, `ConfChange::add` to promote |
| 07 | [`checkquorum`](07-checkquorum/) | With check-quorum on, a leader that loses contact with its majority steps down | `enable_check_quorum`, `isolate`/`heal`, `one_leader_per_term`, `invariants_hold` |
| 08 | [`snapshot`](08-snapshot/) | Compact the log and watch a node that was down catch up from the leader | `compact_leader`, `crash`/`restart`, `committed_agrees`, `invariants_hold` |
| 09 | [`rawnode`](09-rawnode/) | Drive the core by hand: `tick`, collect `Ready`, `store`, `advance`, deliver messages | `RawNode::new`, `tick`, `ready`, `store`, `advance`, `stabilize` |
| 10 | [`storage`](10-storage/) | Persist Raft state to `MemoryStorage` and rebuild a node from it (restart / recover) | `Node::save_into`, `RaftNode::new`, `Node::load_from`, `persisted`, `recover` |
| 11 | [`readindex`](11-readindex/) | A linearizable read: the leader confirms its commit index with a fresh quorum round | `RawNode::read_index`, `Ready::read_states`, `has_ready`, `ready`, `advance` |

Examples `00`-`08` use the deterministic, seeded `Cluster` simulator (output is
reproducible run to run). Examples `09`-`11` use the message-driven `RawNode`,
which is the layer a real deployment wires to a network and a storage backend -
the same consensus core, driven by your own ticks and messages.
