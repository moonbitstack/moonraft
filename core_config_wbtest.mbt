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
/// A `Logger` that records every message for assertions.
struct CapturingLogger {
  records : Array[(LogLevel, String)]
}

///|
fn CapturingLogger::new() -> CapturingLogger {
  { records: [] }
}

///|
impl Logger for CapturingLogger with fn log(self, level, msg) {
  self.records.push((level, msg))
}

///|
fn CapturingLogger::messages(self : CapturingLogger) -> Array[String] {
  let out : Array[String] = []
  for r in self.records {
    out.push(r.1)
  }
  out
}

///|
/// A `Tracer` that records every event for assertions.
struct CapturingTracer {
  events : Array[TraceEvent]
}

///|
fn CapturingTracer::new() -> CapturingTracer {
  { events: [] }
}

///|
impl Tracer for CapturingTracer with fn on_event(self, event) {
  self.events.push(event)
}

///|
fn CapturingTracer::count(self : CapturingTracer) -> Int {
  self.events.length()
}

///|
fn CapturingTracer::state_changes_to(
  self : CapturingTracer,
  role : Role,
) -> Int {
  let mut n = 0
  for e in self.events {
    if e is StateChange(_, r, _) && r == role {
      n = n + 1
    }
  }
  n
}

///|
/// DIVERGENT fix (StepDownOnRemoval): etcd's `switchToConfig` steps a removed
/// leader down only when `Config.StepDownOnRemoval` is set, and that field
/// defaults to false. Before the fix this server stepped down unconditionally.
/// This is the "red" that the old behavior was wrong: with the default the
/// leader that commits its own removal stays leader, exactly as etcd does.
test "StepDownOnRemoval default (false): removed leader does not step down" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  assert_true(r.is_leader())
  r.propose_conf(ConfChange::remove("1")) |> ignore // conf entry @2
  r.step(ack_from("2", 2, r.term())) |> ignore
  r.step(ack_from("3", 2, r.term())) |> ignore
  // The removal committed and applied — node 1 is gone from the configuration —
  // yet it keeps leading, because step-down-on-removal is off by default.
  assert_false(r.config.contains("1"))
  assert_true(r.is_leader())
  assert_true(r.role() == Leader)
}

///|
/// The opt-in path: with the flag set, the same removal steps the leader down
/// (the "green" companion to the default test, and to B2 above).
test "StepDownOnRemoval opt-in (true): removed leader steps down" {
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    step_down_on_removal=true,
  )
  r.node().become_candidate()
  r.become_leader()
  r.propose_conf(ConfChange::remove("1")) |> ignore
  r.step(ack_from("2", 2, r.term())) |> ignore
  r.step(ack_from("3", 2, r.term())) |> ignore
  assert_false(r.config.contains("1"))
  assert_false(r.is_leader())
  assert_true(r.role() == Follower)
}

///|
/// The `Logger` is really wired: a capturing logger installed on the node
/// records the leader-election transition the core logs (etcd logs "became
/// leader at term N" in `becomeLeader`). Proves the trait is not an orphan.
test "Logger records the become-leader transition" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false, logger=log)
  r.node().become_candidate()
  r.become_leader()
  let msgs = log.messages()
  let mut saw_leader = false
  for m in msgs {
    if m == "1 became leader at term 1" {
      saw_leader = true
    }
  }
  assert_true(saw_leader)
}

///|
/// The default `NopLogger` neither perturbs behavior nor records anything: a
/// node built without a logger still elects a leader normally.
test "NopLogger is silent and inert" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  assert_true(r.is_leader())
}

///|
/// The `Tracer` is really wired: a capturing tracer sees the state-change events
/// the core emits (etcd's `traceBecomeLeader`). Proves the trait is not an
/// orphan and that events reach the sink through the live protocol path.
test "Tracer records state transitions and commits" {
  let tr = CapturingTracer::new()
  let r = RaftNode::new("1", [], pre_vote=false, tracer=tr)
  r.node().become_candidate()
  r.become_leader()
  // A single-node leader appends and commits its no-op immediately.
  assert_true(tr.state_changes_to(Leader) >= 1)
  assert_true(tr.count() >= 2) // at least InitState + become-leader
}

///|
/// ReadOnlyOption is a first-class, configurable enum. The server defaults to
/// lease-based reads; `Safe` can be selected at construction or via the setter.
test "ReadOnlyOption is configurable" {
  let r = RaftNode::new("1", ["2", "3"])
  assert_true(r.read_only_option() is LeaseBased)
  r.set_read_only_option(Safe)
  assert_true(r.read_only_option() is Safe)
  let s = RaftNode::new("1", ["2", "3"], read_only_option=Safe)
  assert_true(s.read_only_option() is Safe)
}

///|
/// A stale `ReadIndexResp` (carrying a term below ours) is dropped, never
/// recorded as a confirmed read. This is the response among the routable
/// messages added in f7103f8; it is the only one that carries a term, so it is
/// the one the stale-term guard must reject (the B1 family). The request-side
/// routable messages (Propose/ReadIndex/TransferLeader/ForgetLeader) carry no
/// term and are covered by the forwarding tests below.
test "B1: stale ReadIndexResp is dropped" {
  let f = RaftNode::new("2", ["1", "3"], pre_vote=false)
  // Move the follower to a higher term than the stale reply will carry.
  f.become_follower(5)
  let before = f.take_read_states().length()
  // A ReadIndexResp stamped with an old term (3 < 5) must be ignored.
  f.step(
    Message::new("1", "2", ReadIndexResp({ term: 3, index: 9, context: b"rc" })),
  )
  |> ignore
  assert_eq(f.take_read_states().length(), before)
}

///|
/// A current-term `ReadIndexResp` is accepted and recorded, so the drop above is
/// really the stale-term guard doing its job and not a blanket ignore.
test "current-term ReadIndexResp is recorded" {
  let f = RaftNode::new("2", ["1", "3"], pre_vote=false)
  f.become_follower(5)
  f.step(
    Message::new("1", "2", ReadIndexResp({ term: 5, index: 9, context: b"rc" })),
  )
  |> ignore
  let rs = f.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 9)
}
