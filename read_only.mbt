// Copyright 2015 The etcd Authors
// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
/// One in-flight linearizable read: the caller's opaque context, the leader's
/// commit index captured when the read was requested (Raft §6.4), and the id of
/// the server that originated the read — the leader itself for a local read, or a
/// follower that forwarded a `ReadIndex`, so the confirmed index routes back to
/// the right requester.
priv struct ReadReq {
  context : Bytes
  index : UInt64
  from : String
}

///|
/// The read-index bookkeeping a leader keeps for linearizable reads. In the
/// `ReadOnlySafe` mode a read is only served once a quorum of followers has
/// acknowledged a heartbeat stamped with the read's context, proving the leader
/// still leads *right now*; in `ReadOnlyLeaseBased` mode the leader trusts its
/// election lease instead. Requests are confirmed in FIFO order: a quorum on one
/// context confirms every earlier read too.
struct ReadOnly {
  mut safe : Bool
  pending : Array[ReadReq]
  acks : Map[String, Map[String, Bool]]
  ready : Array[ReadState]
}

///|
/// A fresh, lease-based read tracker (matching etcd's default).
fn ReadOnly::new() -> ReadOnly {
  { safe: false, pending: [], acks: {}, ready: [] }
}

///|
fn ctx_key(context : Bytes) -> String {
  context.to_unchecked_string()
}

///|
/// Record a new read request (originated by `from`) and count the leader's own
/// implicit acknowledgement.
fn ReadOnly::add_request(
  self : ReadOnly,
  index : UInt64,
  context : Bytes,
  from : String,
  leader_id : String,
) -> Unit {
  self.pending.push({ context, index, from })
  let acked : Map[String, Bool] = {}
  acked[leader_id] = true
  self.acks[ctx_key(context)] = acked
}

///|
/// Record that `id` acknowledged the heartbeat carrying `context`. Once a quorum
/// of the configuration has acknowledged it, every read up to and including that
/// context is confirmed and returned in order; the caller routes each confirmed
/// read to its originator.
fn ReadOnly::recv_ack(
  self : ReadOnly,
  id : String,
  context : Bytes,
  config : Membership,
) -> Array[ReadReq] {
  let key = ctx_key(context)
  match self.acks.get(key) {
    None => []
    Some(acked) => {
      acked[id] = true
      let ackers : Array[String] = []
      for k, _ in acked {
        ackers.push(k)
      }
      if config.has_majority(ackers) {
        self.advance(context)
      } else {
        []
      }
    }
  }
}

///|
/// Confirm every pending read up to and including the one carrying `context`,
/// returning them in order and forgetting their ack sets.
fn ReadOnly::advance(self : ReadOnly, context : Bytes) -> Array[ReadReq] {
  let key = ctx_key(context)
  let mut cut = -1
  for i, req in self.pending {
    if ctx_key(req.context) == key {
      cut = i
      break
    }
  }
  if cut < 0 {
    return []
  }
  let confirmed : Array[ReadReq] = []
  let rest : Array[ReadReq] = []
  for i, req in self.pending {
    if i <= cut {
      confirmed.push(req)
      self.acks.remove(ctx_key(req.context))
    } else {
      rest.push(req)
    }
  }
  self.pending.clear()
  for req in rest {
    self.pending.push(req)
  }
  confirmed
}

///|
/// Hand over the reads confirmed since the last call, clearing the buffer.
fn ReadOnly::take_ready(self : ReadOnly) -> Array[ReadState] {
  let out = self.ready.copy()
  self.ready.clear()
  out
}

///|
/// Switch this server to the linearizable `ReadOnlySafe` read mode: a read is
/// confirmed by a fresh heartbeat quorum rather than the election lease.
pub fn RaftNode::enable_read_only_safe(self : RaftNode) -> Unit {
  self.read_only.safe = true
}

///|
/// Request a linearizable read (Raft §6.4). Returns the heartbeats to broadcast
/// (in `ReadOnlySafe` mode) so a quorum can confirm the leader is current; the
/// confirmed read index is later collected with `take_read_states`. A follower,
/// or a leader that has not yet committed an entry in its own term, serves
/// nothing. In `ReadOnlyLeaseBased` mode (the default) a read is confirmed at
/// once when the lease is valid.
pub fn RaftNode::request_read_index(
  self : RaftNode,
  context : Bytes,
) -> Array[Message] {
  if self.core.role() != Leader {
    return []
  }
  self.lead_read_index(self.id, context)
}

///|
/// Serve a read request on the leader (Raft §6.4), for a read originated by
/// `from` — the leader itself for a local read, or a follower that forwarded a
/// `ReadIndex`. In `ReadOnlySafe` mode the read is held until a heartbeat quorum
/// confirms the leadership, so the returned messages are the heartbeats to
/// broadcast; in lease mode a valid lease confirms it at once. A confirmed read
/// is delivered to its originator (recorded locally, or answered with a
/// `ReadIndexResp`).
fn RaftNode::lead_read_index(
  self : RaftNode,
  from : String,
  context : Bytes,
) -> Array[Message] {
  // The read must be anchored at an index from the current term, else the commit
  // index may not yet reflect the latest leader's writes (§5.4.2).
  if self.core.term_at(self.core.commit_index) != self.core.current_term() {
    return []
  }
  if self.peers.length() == 0 {
    return self.deliver_read({ from, index: self.core.commit_index, context })
  }
  if self.read_only.safe {
    self.read_only.add_request(self.core.commit_index, context, from, self.id)
    self.bcast_heartbeat(context~)
  } else if self.quorum_active() {
    self.deliver_read({ from, index: self.core.commit_index, context })
  } else {
    []
  }
}

///|
/// Route a confirmed read to its originator: a local read is recorded for
/// `take_read_states`; a read forwarded by a follower is answered with a
/// `ReadIndexResp` carrying the confirmed index and the caller's context.
fn RaftNode::deliver_read(self : RaftNode, req : ReadReq) -> Array[Message] {
  if req.from == self.id {
    self.read_only.ready.push({ index: req.index, request_ctx: req.context })
    []
  } else {
    [
      Message::new(
        self.id,
        req.from,
        ReadIndexResp({
          term: self.core.current_term(),
          index: req.index,
          context: req.context,
        }),
      ),
    ]
  }
}

///|
/// Collect the linearizable reads confirmed since the last call. Each carries
/// the commit index the state machine must have applied before the read is
/// answered, so it observes every previously-acknowledged write.
pub fn RaftNode::take_read_states(self : RaftNode) -> Array[ReadState] {
  self.read_only.take_ready()
}
