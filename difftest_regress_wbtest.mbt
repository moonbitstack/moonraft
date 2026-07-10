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

// Regressions for divergences the differential trace harness found.

///|
/// difftest 07/08/09: a configuration change counts toward the uncommitted-log
/// quota, exactly as a normal proposal does (etcd's appendEntry always calls
/// increaseUncommittedSize). Previously propose_conf bypassed the accounting.
test "difftest 07: a conf change counts toward the uncommitted quota" {
  let r = cov_leader()
  let before = r.uncommitted_size()
  r.propose_conf(ConfChange::add("4")) |> ignore
  assert_true(r.uncommitted_size() > before)
}

///|
/// difftest 05: stepping down (or any state transition) resets the uncommitted-
/// tail accumulator (etcd's reset()). Previously an ex-leader that truncated its
/// conflicting tail and became a follower left the tally non-zero.
test "difftest 05: stepping down resets the uncommitted tally" {
  let r = cov_leader()
  r.propose(b"data") |> ignore
  assert_true(r.uncommitted_size() > 0)
  r.become_follower(r.term() + 1)
  assert_eq(r.uncommitted_size(), 0)
}

///|
/// A conf change proposed on a follower is forwarded to the leader (like a
/// normal proposal), not silently dropped — a consequence of routing propose_conf
/// through the shared MsgProp path.
test "a follower forwards a conf change to the leader" {
  let f = follower_of_leader_1("2", ["1", "3"])
  let out = f.propose_conf(ConfChange::add("4"))
  assert_eq(out.length(), 1)
  match out[0].payload {
    Propose(_) => assert_eq(out[0].to, "1")
    _ => abort("expected a forwarded Propose")
  }
}
