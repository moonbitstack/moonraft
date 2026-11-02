// Copyright 2019 The etcd Authors
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
// A running scoreboard for the 28 raft scripts: every case is replayed through
// the harness and compared with the block upstream wrote down. The count only
// moves up -- the floor below is raised whenever another script converges, so a
// change that stops a script reproducing upstream fails here instead of passing
// silently.

///|
/// How many of the 28 raft scripts' cases already answer exactly what upstream
/// wrote. Blocks still missing are the ones whose text comes from a core log
/// line this port does not emit yet, or whose Ready ordering differs.
test "datadriven: raft scripts reproduce upstream block by block" {
  let mut total = 0
  let mut matched = 0
  for entry in dd_scripts {
    let (kind, _name, script) = entry
    if kind != "raft" {
      continue
    }
    let rt = RaftTest::new()
    for c in dd_parse_script(script) {
      total = total + 1
      let got = rt.run(c.cmd, c.input)
      if got.trim_end() == c.expected.trim_end() {
        matched = matched + 1
      }
    }
  }
  // The corpus of 28 raft scripts holds 558 cases in total.
  assert_eq(total, 558)
  assert_true(matched >= 453)
}
