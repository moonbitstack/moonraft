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
/// Transferring leadership to a learner is refused outright (etcd checks
/// pr.IsLearner first), without disturbing any pending transfer.
test "cov: transfer to a learner is a no-op" {
  let r = cov_leader()
  r.config.members.retain(fn(m) { m != "2" })
  r.config.add_learner("2")
  let out = r.transfer_leadership("2")
  assert_eq(out.length(), 0)
  assert_true(r.lead_transferee == None)
}

///|
/// advance_applied with an index at or below the current applied watermark is a
/// no-op (it never moves the watermark backwards).
test "cov: advance_applied with a stale index is a no-op" {
  let r = cov_leader()
  r.advance_applied(0)
  assert_true(r.is_leader())
}

///|
/// bcast_append withholds a snapshot from a follower that is not recently active
/// (etcd's maybeSendSnapshot !RecentActive), so no message goes to it.
test "cov: bcast withholds a snapshot from an inactive follower" {
  let r = snap_leader()
  let p = r.progress["2"]
  p.next_index = 11 // needs the compacted prefix → snapshot territory
  p.msg_app_flow_paused = false
  assert_false(p.is_active())
  assert_eq(r.bcast_append().length(), 0)
}
