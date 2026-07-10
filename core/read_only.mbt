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
/// How a linearizable read is confirmed (etcd's `ReadOnlyOption`).
///
/// `Safe` confirms every read with a fresh heartbeat quorum, so it holds even
/// under unbounded clock drift — etcd's default and recommended setting.
/// `LeaseBased` trusts the leader's election lease instead, saving the round
/// trip at the cost of depending on bounded clock drift; etcd requires
/// check-quorum to be on when it is selected.
pub enum ReadOnlyOption {
  Safe
  LeaseBased
} derive(Eq)

///|
/// Select how this leader confirms linearizable reads (etcd's
/// `Config.ReadOnlyOption`). `Safe` is etcd's default; this server defaults to
/// `LeaseBased` for backward compatibility with existing callers, so a
/// deployment that wants etcd's default must ask for `Safe` explicitly.
pub fn RaftNode::set_read_only_option(
  self : RaftNode,
  option : ReadOnlyOption,
) -> Unit {
  self.read_only.safe = option is Safe
}

///|
/// The read-confirmation mode this server is currently using.
pub fn RaftNode::read_only_option(self : RaftNode) -> ReadOnlyOption {
  if self.read_only.safe {
    Safe
  } else {
    LeaseBased
  }
}

///|
/// One in-flight linearizable read: the caller's opaque context, the leader's
/// commit index captured when the read was requested (Raft §6.4), and the id of
/// the server that originated the read — the leader itself for a local read, or a
/// follower that forwarded a `ReadIndex`, so the confirmed index routes back to
/// the right requester.
priv struct ReadReq {
  // The caller's opaque context, echoed back with the confirmed index so the
  // originator can match the answer to its request.
  context : Bytes
  index : UInt64
  from : String
}

///|
/// The read-index bookkeeping a leader keeps for linearizable reads (etcd's
/// `readOnly`, current design). In `ReadOnlySafe` mode confirmation is by an
/// internal *position* counter, not the caller's context: each heartbeat carries
/// the position `confirmed + len(unconfirmed)`, so a single quorum acknowledgement
/// releases every currently-unconfirmed read at once. This is what etcd switched
/// to (see its "use an internally defined context" note) and avoids the collision
/// where two reads sharing a context would clobber each other's ack state. In
/// `ReadOnlyLeaseBased` mode the leader trusts its election lease instead.
struct ReadOnly {
  mut safe : Bool
  // Each voter → the highest read-confirmation position it has acknowledged.
  acks : Map[String, UInt64]
  // Reads awaiting quorum confirmation, in request order.
  unconfirmed : Array[ReadReq]
  // How many reads have already been confirmed and drained from the front.
  mut confirmed : UInt64
  ready : Array[ReadState]
}

///|
/// A fresh, lease-based read tracker (matching etcd's default).
fn ReadOnly::new() -> ReadOnly {
  { safe: false, acks: {}, unconfirmed: [], confirmed: 0, ready: [] }
}

///|
/// The 8-byte little-endian read-confirmation position to stamp into a heartbeat
/// (etcd's `heartbeatCtx`), so a quorum ack confirms every currently-unconfirmed
/// read at once. Empty when there is nothing to confirm.
fn ReadOnly::heartbeat_ctx(self : ReadOnly) -> Bytes {
  if self.unconfirmed.length() == 0 {
    return b""
  }
  encode_u64_le(self.confirmed + self.unconfirmed.length().to_uint64())
}

///|
/// Record a read request originated by `from` (etcd's `addRequest`): just append
/// — confirmation is positional, so no per-read ack set is kept.
fn ReadOnly::add_request(
  self : ReadOnly,
  index : UInt64,
  context : Bytes,
  from : String,
) -> Unit {
  self.unconfirmed.push({ context, index, from })
}

///|
/// Record that voter `id` acknowledged a heartbeat carrying position `ctx`
/// (etcd's `recvAck`): keep the highest position each voter has confirmed.
fn ReadOnly::recv_ack(self : ReadOnly, id : String, ctx : Bytes) -> Unit {
  if ctx.length() != 0 {
    let pos = decode_u64_le(ctx)
    let cur = self.acks.get(id).unwrap_or(0)
    if pos > cur {
      self.acks[id] = pos
    }
  }
}

///|
/// Confirm as many reads as the quorum has now acknowledged (etcd's
/// `maybeAdvance`): the quorum-committed position over the per-voter acks
/// releases that many reads from the front of `unconfirmed`, in order.
fn ReadOnly::maybe_advance(
  self : ReadOnly,
  config : Membership,
) -> Array[ReadReq] {
  let new_confirmed = config.committed_index(self.acks)
  if new_confirmed <= self.confirmed {
    return []
  }
  let n = (new_confirmed - self.confirmed).to_int()
  let released : Array[ReadReq] = []
  let rest : Array[ReadReq] = []
  for i, req in self.unconfirmed {
    if i < n {
      released.push(req)
    } else {
      rest.push(req)
    }
  }
  self.unconfirmed.clear()
  for req in rest {
    self.unconfirmed.push(req)
  }
  self.confirmed = new_confirmed
  released
}

///|
/// Encode a 64-bit value as 8 little-endian bytes (etcd uses
/// `binary.LittleEndian` for the read-confirmation position).
fn encode_u64_le(v : UInt64) -> Bytes {
  let arr : Array[Byte] = []
  for i in 0..<8 {
    arr.push(((v >> (i * 8)) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// Decode 8 little-endian bytes back to a 64-bit value.
fn decode_u64_le(b : Bytes) -> UInt64 {
  let mut v = 0UL
  let n = if b.length() < 8 { b.length() } else { 8 }
  for i in 0..<n {
    v = v | (b[i].to_uint64() << (i * 8))
  }
  v
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
  // A singleton leader (its only voter) has its leadership trivially confirmed,
  // so the read is answered at once against the commit index — etcd's
  // `IsSingleton` fast path, taken before the in-term-commit gate below.
  if self.peers.length() == 0 {
    return self.deliver_read({ from, index: self.core.commit_index, context })
  }
  // The read must be anchored at an index from the current term, else the commit
  // index may not yet reflect the latest leader's writes (§5.4.2). Until the
  // leader has committed in its term the request is *queued*, not dropped: etcd
  // holds it in `pendingReadIndexMessages` and releases it on the first in-term
  // commit, so a read issued right after an election is answered rather than
  // lost.
  if self.core.term_at(self.core.commit_index) != self.core.current_term() {
    self.pending_read_index.push((from, context))
    return []
  }
  if self.read_only.safe {
    self.read_only.add_request(self.core.commit_index, context, from)
    // The leader implicitly acknowledges the new position itself (etcd's
    // recvAck(r.id, heartbeatCtx)), then broadcasts a heartbeat stamped with it.
    self.read_only.recv_ack(self.id, self.read_only.heartbeat_ctx())
    self.bcast_heartbeat()
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
/// Answer the read-index requests that were queued before the leader had
/// committed an entry in its current term, now that it has (etcd's
/// `releasePendingReadIndexMessages`). Returns any messages the served reads
/// produce (heartbeats to confirm leadership under `ReadOnlySafe`). A no-op
/// while the queue is empty or the leader still has no in-term commit.
fn RaftNode::release_pending_read_index(self : RaftNode) -> Array[Message] {
  if self.pending_read_index.length() == 0 {
    return []
  }
  // The sole caller runs this immediately after `maybe_commit` reported a fresh
  // commit, which by §5.4.2 only advances the commit index onto a current-term
  // entry, so the in-term-commit precondition (etcd's `committedEntryInCurrentTerm`
  // guard in `releasePendingReadIndexMessages`) always already holds here.
  let pending = self.pending_read_index.copy()
  self.pending_read_index.clear()
  let out : Array[Message] = []
  for req in pending {
    for m in self.lead_read_index(req.0, req.1) {
      out.push(m)
    }
  }
  out
}

///|
/// Collect the linearizable reads confirmed since the last call. Each carries
/// the commit index the state machine must have applied before the read is
/// answered, so it observes every previously-acknowledged write.
pub fn RaftNode::take_read_states(self : RaftNode) -> Array[ReadState] {
  self.read_only.take_ready()
}
