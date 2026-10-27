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
/// DIVERGENT fix (D-readindex-queue): etcd postpones a MsgReadIndex when the
/// leader has not yet committed an entry in its current term, queuing it in
/// `pendingReadIndexMessages` and releasing it once `maybeCommit` advances the
/// commit index into the term (`raft.go:1365,1553`). This node previously
/// dropped such reads outright. The red case: a freshly elected leader whose
/// no-op is not yet committed is asked for a read; before the fix the read is
/// lost forever, after it the read is served once the no-op commits.
test "D-readindex-queue: read before first in-term commit is queued, not dropped" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.set_read_only_option(LeaseBased) // this case exercises the lease read path
  r.node().become_candidate() // term 1
  r.become_leader() // appends no-op @1; commit still 0 (no acks yet)
  // The no-op is not yet committed in the current term.
  assert_true(r.core.term_at(r.commit_index()) != r.term())
  // A read requested now is held, not answered: nothing is emitted or recorded.
  let msgs = r.request_read_index(b"rc")
  assert_eq(msgs.length(), 0)
  assert_eq(r.take_read_states().length(), 0)
  // Node 2 acks the no-op @1, committing it in the current term. The queued read
  // is released and served against the now-current commit index.
  r.step(ack_from("2", 1, r.term())) |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
}

///|
/// Once the leader has committed in its term, a read is served immediately (the
/// queue is only for the pre-commit window), so the fix does not delay normal
/// reads.
test "D-readindex-queue: read after in-term commit is served at once" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.set_read_only_option(LeaseBased) // this case exercises the lease read path
  r.node().become_candidate()
  r.become_leader()
  r.step(ack_from("2", 1, r.term())) |> ignore // commit the no-op first
  assert_true(r.core.term_at(r.commit_index()) == r.term())
  r.request_read_index(b"rc") |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
}
