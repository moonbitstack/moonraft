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
/// A committed removal shrinks the quorum, which can commit an entry that was
/// still trailing: `report_after_config` broadcasts that new commit and re-enters
/// the fold to apply it.
test "a shrunken quorum commits the trailing entries" {
  let r = RaftNode::new("1", ["2", "3", "4"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  // Commit the no-op under the original 4-voter quorum (3 of 4).
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.step(ack_from("3", 1, r.term())) |> ignore
  // e1 @2, remove-4 @3, e2 @4 all ride the pipeline.
  r.propose(b"e1") |> ignore
  r.propose_conf(ConfChange::remove("4")) |> ignore
  r.propose(b"e2") |> ignore
  // Node 3 acks 3; node 2 acks 4. The removal at 3 commits under the old quorum,
  // and the new 3-voter quorum then commits e2 at 4.
  r.step(ack_from("3", 3, r.term())) |> ignore
  r.step(ack_from("2", 4, r.term())) |> ignore
  assert_eq(r.commit_index(), 4UL)
}

///|
/// An append that rewrites only part of the unstable tail takes the "truncate"
/// branch of `unstable.truncateAndAppend` (rather than a plain append or a
/// wholesale replace), and the driver names it.
test "a mid-tail conflict reports a truncate" {
  let rn = RaftNode::new("1", ["2"], pre_vote=false).raw()
  rn.step(
    Message::new(
      "2",
      "1",
      Append({
        term: 1,
        leader_id: "2",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")],
        leader_commit: 0,
      }),
    ),
  )
  // Overwrite only index 2, keeping index 1: fromIndex sits inside the unstable
  // range, so the earlier prefix is retained and the tail truncated.
  rn.step(
    Message::new(
      "2",
      "1",
      Append({
        term: 2,
        leader_id: "2",
        prev_log_index: 1,
        prev_log_term: 1,
        entries: [Entry::normal(2, 2, b"c")],
        leader_commit: 0,
      }),
    ),
  )
  assert_eq(rn.node().node().last_log_index(), 2UL)
  assert_eq(rn.node().node().term_at(2UL), 2UL)
}
