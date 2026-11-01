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
// Minimal harnesses for the two other datadriven suites archived under
// testdata/etcd/: the quorum package's `vote`/`committed` scripts and the
// confchange package's `simple`/`enter-joint`/`leave-joint` scripts. They drive
// the real quorum / confchange computations and render their results.

///|
/// Parse a comma list such as `1,2,3` (parentheses already stripped by
/// `parse_args`); the literal `zero` and the empty string mean "no nodes".
fn parse_list(s : String) -> Array[String] {
  let t = s.trim().to_owned()
  if t == "" || t == "zero" {
    []
  } else {
    split_str(t, ",")
  }
}

///|
/// Parse an unsigned decimal from a string of digits.
fn parse_u64(s : String) -> UInt64 {
  let chars = s.trim().to_owned().to_array()
  let mut n = 0UL
  for c in chars {
    n = n * 10UL + (c.to_int() - 48).to_uint64()
  }
  n
}

///|
/// Split on any whitespace (spaces and newlines).
fn split_ws(s : String) -> Array[String] {
  s
  .replace(old="\n", new=" ")
  .split(" ")
  .map(fn(x : StringView) { x.to_owned() })
  .filter(fn(x) { x.trim() != "" })
  .to_array()
}

// ---------------------------------------------------------------------------
// Quorum: vote and committed
// ---------------------------------------------------------------------------

///|
/// The quorum datadriven harness (quorum/testdata/*.txt). Stateless: each case
/// computes a vote result or a committed index from its own arguments.
pub fn run_quorum(line : String, _input : String) -> String {
  let a = parse_args(line)
  match a.cmd {
    "vote" => {
      let cfg = parse_list(kv_get(a.kv, "cfg", ""))
      let vs = parse_list(kv_get(a.kv, "votes", ""))
      let votes : Map[String, Bool] = Map([], capacity=8)
      for i = 0; i < cfg.length(); i = i + 1 {
        if i < vs.length() {
          let v = vs[i]
          if v == "y" {
            votes[cfg[i]] = true
          } else if v == "n" {
            votes[cfg[i]] = false
          }
        }
      }
      match @quorum.vote_result(cfg, [], votes) {
        @quorum.VoteState::VoteWon => "VoteWon"
        @quorum.VoteState::VoteLost => "VoteLost"
        @quorum.VoteState::VotePending => "VotePending"
      }
    }
    "committed" => {
      let cfg = parse_list(kv_get(a.kv, "cfg", ""))
      let cfgj = parse_list(kv_get(a.kv, "cfgj", ""))
      let idxs = parse_list(kv_get(a.kv, "idx", ""))
      let ids : Array[String] = []
      for x in cfg {
        if !ids.contains(x) {
          ids.push(x)
        }
      }
      for x in cfgj {
        if !ids.contains(x) {
          ids.push(x)
        }
      }
      let acked : Map[String, UInt64] = Map([], capacity=8)
      for i = 0; i < ids.length(); i = i + 1 {
        if i < idxs.length() {
          let v = idxs[i]
          if v != "_" && v != "" {
            acked[ids[i]] = parse_u64(v)
          }
        }
      }
      "\{@quorum.committed_index(cfg, cfgj, acked)}"
    }
    _ => "unsupported: \{a.cmd}"
  }
}

// ---------------------------------------------------------------------------
// ConfChange: simple, enter-joint, leave-joint
// ---------------------------------------------------------------------------

///|
/// The confchange datadriven harness (confchange/testdata/*.txt).
pub struct ConfChangeTest {
  changer : @confchange.Changer
}

///|
pub fn ConfChangeTest::new() -> ConfChangeTest {
  { changer: @confchange.Changer::new(), }
}

///|
/// Parse a change list like `v1 r0 v0 l0` into (kind, id) pairs.
fn parse_changes(input : String) -> Array[(String, String)] {
  let toks = split_ws(input)
  let out : Array[(String, String)] = []
  for t in toks {
    let chars = t.trim().to_owned().to_array()
    if chars.length() >= 2 {
      let kind = match chars[0] {
        'v' => "v"
        'r' => "r"
        'l' => "l"
        _ => "v"
      }
      let idb = StringBuilder()
      for i = 1; i < chars.length(); i = i + 1 {
        idb.write_char(chars[i])
      }
      out.push((kind, idb.to_string()))
    }
  }
  out
}

///|
pub fn ConfChangeTest::run(
  self : ConfChangeTest,
  line : String,
  input : String,
) -> String {
  let a = parse_args(line)
  let changes = parse_changes(input)
  match a.cmd {
    "simple" => self.emit(self.changer.simple(changes))
    "enter-joint" => {
      // `autoleave` is a directive argument in etcd's scripts (and, in some,
      // part of the block input); accept either.
      let auto = kv_get(a.kv, "autoleave", "false") == "true" ||
        input.contains("autoleave")
      self.emit(self.changer.enter_joint(auto, changes))
    }
    "leave-joint" => self.emit(self.changer.leave_joint())
    _ => "unsupported: \{a.cmd}"
  }
}

///|
fn ConfChangeTest::emit(self : ConfChangeTest, res : String?) -> String {
  match res {
    Some(e) => "error: \{e}"
    None => self.changer.describe().trim_end().to_owned()
  }
}
