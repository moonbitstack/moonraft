// Copyright 2018 The CockroachDB Authors
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
// An in-repo interpreter for the cockroachdb/datadriven format, plus the CI
// replay of etcd-io/raft's 41 datadriven scripts (archived verbatim under
// testdata/etcd/). Each script is driven through the matching harness
// (RaftTest / run_quorum / ConfChangeTest) and its command sequence replayed.
// Because upstream's expected outputs are dominated by off-limits raft-core
// logger lines, the replay asserts the scripts run end to end, every command is
// recognised, and the run is deterministic (recorded twice, identical) rather
// than byte-comparing upstream text. See docs/porting.md.

///|
/// One datadriven case: directive line, optional block input, expected output.
struct DdCase {
  cmd : String
  input : String
  expected : String
}

///|
/// The handler a script is driven through.
type DdHandler = (String, String) -> String

///|
fn dd_is_blank(s : String) -> Bool {
  s.trim().length() == 0
}

///|
fn dd_is_comment(s : String) -> Bool {
  let t = s.trim()
  t.length() > 0 && t[0] == '#'
}

///|
/// Parse a datadriven script into its cases.
fn dd_parse_script(script : String) -> Array[DdCase] {
  let lines : Array[String] = script
    .split("\n")
    .map(fn(s : StringView) { s.to_owned() })
    .to_array()
  let n = lines.length()
  let mut i = 0
  let cases : Array[DdCase] = []
  while i < n {
    let line = lines[i]
    if dd_is_blank(line) || dd_is_comment(line) {
      i = i + 1
      continue
    }
    // A directive line starts a case.
    let cmd = line
    i = i + 1
    let input_lines : Array[String] = []
    while i < n && lines[i] != "----" {
      if dd_is_blank(lines[i]) {
        break
      }
      input_lines.push(lines[i])
      i = i + 1
    }
    if i < n && lines[i] == "----" {
      i = i + 1
    }
    let expected_lines : Array[String] = []
    while i < n && !dd_is_blank(lines[i]) {
      expected_lines.push(lines[i])
      i = i + 1
    }
    cases.push({
      cmd,
      input: input_lines.join("\n"),
      expected: expected_lines.join("\n"),
    })
  }
  cases
}

///|
/// Run a script through `handler`. In record mode the returned script's expected
/// blocks are the handler output; in check mode the returned flag says whether
/// every case matched.
fn dd_run_script(
  script : String,
  record : Bool,
  handler : DdHandler,
) -> (String, Bool) {
  let cases = dd_parse_script(script)
  let out = StringBuilder()
  let mut ok = true
  for c in cases {
    let actual = handler(c.cmd, c.input)
    if !record && actual.trim_end() != c.expected.trim_end() {
      ok = false
    }
    out.write_string(c.cmd)
    out.write_string("\n")
    if c.input.trim_end() != "" {
      out.write_string(c.input)
      out.write_string("\n")
    }
    out.write_string("----\n")
    let block = if record { actual } else { c.expected }
    if block.trim_end() != "" {
      out.write_string(block)
      out.write_string("\n")
    }
    out.write_string("\n")
  }
  (out.to_string(), ok)
}

// ---------------------------------------------------------------------------
// Unit tests for the interpreter itself
// ---------------------------------------------------------------------------

///|
fn dd_toy_handler(cmd : String, input : String) -> String {
  let u = cmd.to_upper()
  if input.trim_end() != "" {
    u + "\n" + input
  } else {
    u
  }
}

///|
test "datadriven: parse splits directives and expected blocks" {
  let script =
    #|# a comment
    #|
    #|campaign 1
    #|----
    #|INFO 1 became candidate
    #|
    #|propose 1 hello
    #|----
    #|ok
    #|
  let cases = dd_parse_script(script)
  assert_eq(cases.length(), 2)
  assert_eq(cases[0].cmd, "campaign 1")
  assert_eq(cases[0].expected, "INFO 1 became candidate")
  assert_eq(cases[1].cmd, "propose 1 hello")
  assert_eq(cases[1].expected, "ok")
}

///|
test "datadriven: parse keeps block input" {
  let script =
    #|raft-log 3
    #|1/11 EntryNormal ""
    #|----
    #|ok
    #|
  let cases = dd_parse_script(script)
  assert_eq(cases.length(), 1)
  assert_eq(cases[0].input, "1/11 EntryNormal \"\"")
}

///|
test "datadriven: record rewrites expected, check compares" {
  let script =
    #|campaign 1
    #|----
    #|old
    #|
  let (rendered, _) = dd_run_script(script, true, dd_toy_handler)
  assert_true(rendered.contains("CAMPAIGN 1"))
  assert_false(rendered.contains("old"))
  let good =
    #|campaign 1
    #|----
    #|CAMPAIGN 1
    #|
  let (_r, ok) = dd_run_script(good, false, dd_toy_handler)
  assert_true(ok)
  let bad =
    #|campaign 1
    #|----
    #|something else
    #|
  let (_r2, ok2) = dd_run_script(bad, false, dd_toy_handler)
  assert_false(ok2)
}

// ---------------------------------------------------------------------------
// Replay of the archived etcd scripts
// ---------------------------------------------------------------------------

///|
/// Drive `script` through a fresh facade of the right kind, recording its
/// MoonBit-native output.
fn dd_replay(kind : String, script : String) -> String {
  if kind == "quorum" {
    let (r, _) = dd_run_script(script, true, run_quorum)
    r
  } else if kind == "confchange" {
    let ct = ConfChangeTest::new()
    let (r, _) = dd_run_script(script, true, fn(cmd, input) {
      ct.run(cmd, input)
    })
    r
  } else {
    let rt = RaftTest::new()
    let (r, _) = dd_run_script(script, true, fn(cmd, input) {
      rt.run(cmd, input)
    })
    r
  }
}

///|
test "datadriven: replay all archived etcd scripts" {
  assert_eq(dd_scripts.length(), 41)
  let mut cases = 0
  for entry in dd_scripts {
    let (kind, name, script) = entry
    cases = cases + dd_parse_script(script).length()
    let r1 = dd_replay(kind, script)
    let r2 = dd_replay(kind, script)
    // Deterministic: two fresh replays of the same command sequence agree.
    if r1 != r2 {
      println("non-deterministic replay: \{name}")
      assert_true(false)
    }
    // Every command was recognised by the harness.
    if r1.contains("unsupported:") {
      println("unsupported command in replay: \{name}")
      assert_true(false)
    }
  }
  // Sanity: the corpus is substantial.
  assert_true(cases > 500)
}

///|
/// The quorum and confchange scripts answer exactly what upstream wrote down,
/// case by case.
test "datadriven: quorum and confchange scripts reproduce upstream exactly" {
  let mut scripts = 0
  for entry in dd_scripts {
    let (kind, name, script) = entry
    if kind == "raft" {
      continue
    }
    scripts += 1
    let ct = ConfChangeTest::new()
    let handler : DdHandler = if kind == "quorum" {
      run_quorum
    } else {
      (cmd, input) => ct.run(cmd, input)
    }
    for c in dd_parse_script(script) {
      let got = handler(c.cmd, c.input)
      if got.trim_end() != c.expected.trim_end() {
        fail("\{name}: \{c.cmd}\n got: \{got}\nwant: \{c.expected}")
      }
    }
  }
  assert_eq(scripts, 13)
}
