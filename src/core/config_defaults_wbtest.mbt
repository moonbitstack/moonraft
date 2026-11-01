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
/// etcd's `Config.PreVote` is opt-in; its zero value is `false`, and this port
/// keeps that default. A configuration built without an explicit `pre_vote` must
/// come up with pre-vote off, so the precandidate round is not run. Pinning the
/// default here means a later change cannot silently enable pre-vote.
test "pre_vote defaults to false" {
  let c = Config::new("1", ["2", "3"])
  assert_false(c.pre_vote)
  // The explicit flag still round-trips, so the field checked above is the real
  // one and not a constant the constructor forgets to set.
  assert_true(Config::new("1", ["2", "3"], pre_vote=true).pre_vote)
}

///|
/// The default off also shows up in behaviour: past the election timeout a node
/// with the default config becomes a plain candidate, skipping the precandidate
/// state that pre-vote would insert first.
test "default config skips the precandidate round" {
  let r = RaftNode::new("1", ["2", "3"])
  // Past the default election timeout (10 ticks) the follower must have started a
  // real election, not a pre-vote.
  for _ in 0..<15 {
    ignore(r.tick())
  }
  assert_true(r.node().role() == Candidate)
}
