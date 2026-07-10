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
// Exercises the util.go debug renderers ported into the log layer:
// DescribeHardState / DescribeConfState / DescribeSnapshot / DescribeEntries,
// payloadsSize, and raftLog.String(). (Ids render as Strings here, not uint64,
// so the format follows etcd's shape over this port's types.)

///|
test "describe_hard_state" {
  assert_eq(
    describe_hard_state({ term: 1, vote: Some("2"), commit: 3 }),
    "Term:1 Vote:2 Commit:3",
  )
  // No vote cast: the Vote field is omitted.
  assert_eq(
    describe_hard_state({ term: 5, vote: None, commit: 0 }),
    "Term:5 Commit:0",
  )
}

///|
test "describe_conf_state" {
  let cs : ConfState = {
    voters: ["1", "2"],
    learners: ["3"],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: true,
  }
  assert_eq(
    describe_conf_state(cs),
    "Voters:[1 2] VotersOutgoing:[] Learners:[3] LearnersNext:[] AutoLeave:true",
  )
}

///|
test "describe_snapshot" {
  let snap : Snapshot = {
    last_index: 4,
    last_term: 2,
    data: b"",
    conf_state: {
      voters: ["1"],
      learners: [],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
  }
  assert_eq(
    describe_snapshot(snap),
    "Index:4 Term:2 ConfState:Voters:[1] VotersOutgoing:[] Learners:[] LearnersNext:[] AutoLeave:false",
  )
}

///|
test "describe_entries" {
  let out = describe_entries([ent(1, 1), ent(2, 1)][:], None)
  assert_eq(out, "1/1 EntryNormal \"\"\n1/2 EntryNormal \"\"\n")
}

///|
test "payloads_size" {
  assert_eq(
    payloads_size([Entry::normal(1, 1, b"ab"), Entry::normal(1, 2, b"c")][:]),
    3,
  )
  assert_eq(payloads_size([Entry::normal(1, 1, b"")][:]), 0)
}

///|
test "raftLog to_string" {
  let l = RaftLog::new(MemoryStorage::new())
  assert_eq(
    l.to_string(),
    "committed=0, applied=0, applying=0, unstable.offset=1, unstable.offsetInProgress=1, len(unstable.Entries)=0",
  )
}
