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
//
// The diagnostics an operator reads while a cluster runs: which election a
// server refused to join and why, which vote it cast, whose term it adopted, and
// how many votes a candidate has. Each case pins the wording, because these
// lines are the ones a datadriven script compares and an operator greps.

///|
/// Whether the logger recorded a line containing `needle`.
fn logged(log : CapturingLogger, needle : String) -> Bool {
  for m in log.messages() {
    if m.contains(needle) {
      return true
    }
  }
  false
}

///|
test "diagnostics: a leader ignores a campaign and says so" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", [], pre_vote=false, logger=log)
  r.campaign() |> ignore
  assert_true(r.is_leader())
  r.campaign() |> ignore
  assert_true(logged(log, "1 ignoring MsgHup because already leader"))
}

///|
test "diagnostics: a learner cannot campaign" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("3", ["1", "2"], pre_vote=false, logger=log)
  r.config.add_learner("3")
  r.campaign() |> ignore
  assert_true(logged(log, "3 is unpromotable and can not campaign"))
}

///|
test "diagnostics: a pending configuration change defers an election" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false, logger=log)
  // Committed but unapplied: the configuration the node would campaign under is
  // not the one it holds, so `hup` refuses and says which term it refused in.
  r.node().append_conf(1UL, b"\x00") |> ignore
  r.node().advance_commit(1UL)
  r.campaign() |> ignore
  assert_true(
    logged(
      log, "1 cannot campaign at term 0 since there are still pending configuration changes to apply",
    ),
  )
}

///|
test "diagnostics: a campaign announces itself and each solicitation" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false, logger=log)
  r.campaign() |> ignore
  assert_true(logged(log, "1 is starting a new election at term 0"))
  assert_true(logged(log, "1 became candidate at term 1"))
  assert_true(
    logged(log, "1 [logterm: 0, index: 0] sent MsgVote request to 2 at term 1"),
  )
  assert_true(
    logged(log, "1 [logterm: 0, index: 0] sent MsgVote request to 3 at term 1"),
  )
}

///|
test "diagnostics: a pre-vote campaign announces the extra round" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true, logger=log)
  r.campaign() |> ignore
  assert_true(logged(log, "1 became pre-candidate at term 0"))
  assert_true(
    logged(
      log, "1 [logterm: 0, index: 0] sent MsgPreVote request to 2 at term 0",
    ),
  )
}

///|
test "diagnostics: a cast vote names both log positions" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false, logger=log)
  let _ = r.step(
    Message::new(
      "1",
      "2",
      Vote({
        term: 1UL,
        candidate_id: "1",
        last_log_index: 0UL,
        last_log_term: 0UL,
      }),
    ),
  )
  assert_true(
    logged(
      log, "2 [logterm: 0, index: 0, vote: 0] cast MsgVote for 1 [logterm: 0, index: 0] at term 1",
    ),
  )
}

///|
test "diagnostics: a refused vote is named as a rejection" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false, logger=log)
  // Give the follower a log the candidate cannot match.
  r.node().append(2UL, b"x") |> ignore
  r.node().become_follower(2UL)
  let _ = r.step(
    Message::new(
      "1",
      "2",
      Vote({
        term: 3UL,
        candidate_id: "1",
        last_log_index: 0UL,
        last_log_term: 0UL,
      }),
    ),
  )
  assert_true(
    logged(
      log, "2 [logterm: 2, index: 1, vote: 0] rejected MsgVote for 1 [logterm: 0, index: 0] at term 3",
    ),
  )
}

///|
test "diagnostics: a candidate counts every vote response" {
  let log = CapturingLogger::new()
  // Five voters: two grants are not yet a majority, so the tally keeps moving.
  let r = RaftNode::new("1", ["2", "3", "4", "5"], pre_vote=false, logger=log)
  r.campaign() |> ignore
  let _ = r.step(
    Message::new("2", "1", VoteResp({ term: 1UL, vote_granted: true, })),
  )
  assert_true(logged(log, "1 received MsgVoteResp from 2 at term 1"))
  assert_true(
    logged(log, "1 has received 2 MsgVoteResp votes and 0 vote rejections"),
  )
  let _ = r.step(
    Message::new("3", "1", VoteResp({ term: 1UL, vote_granted: false, })),
  )
  assert_true(logged(log, "1 received MsgVoteResp rejection from 3 at term 1"))
  assert_true(
    logged(log, "1 has received 2 MsgVoteResp votes and 1 vote rejections"),
  )
}

///|
test "diagnostics: a pre-vote response is counted too" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true, logger=log)
  r.campaign() |> ignore
  let _ = r.step(
    Message::new("2", "1", PreVoteResp({ term: 0UL, vote_granted: true, })),
  )
  assert_true(logged(log, "1 received MsgPreVoteResp from 2 at term 0"))
}

///|
test "diagnostics: adopting a higher term names the message and both terms" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false, logger=log)
  let _ = r.step(
    Message::new(
      "1",
      "2",
      Append({
        term: 4UL,
        leader_id: "1",
        prev_log_index: 0UL,
        prev_log_term: 0UL,
        entries: [],
        leader_commit: 0UL,
      }),
    ),
  )
  assert_true(
    logged(
      log, "2 [term: 0] received a MsgApp message with higher term from 1 [term: 4]",
    ),
  )
}

///|
test "diagnostics: a stale message is named and noted as ignored" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false, logger=log)
  r.become_candidate()
  // A heartbeat from a term the follower has already left.
  let _ = r.step(
    Message::new(
      "1",
      "2",
      Heartbeat({ term: 0UL, leader_id: "1", commit: 0UL, context: b"", }),
    ),
  )
  assert_true(
    logged(
      log, "2 [term: 1] ignored a MsgHeartbeat message with lower term from 1 [term: 0]",
    ),
  )
}
