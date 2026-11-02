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
/// A commit index as etcd's `quorum.Index` prints it: the largest value, which
/// an empty majority reports, is infinity.
fn index_text(i : UInt64) -> String {
  if i == 18446744073709551615UL {
    "∞"
  } else {
    i.to_string()
  }
}

///|
/// `ids` then `idsj`, each id once, in the order they first appear.
fn union_ids(ids : Array[String], idsj : Array[String]) -> Array[String] {
  let out : Array[String] = []
  for id in [..ids, ..idsj] {
    if !out.contains(id) {
      out.push(id)
    }
  }
  out
}

///|
/// The values of `input` assigned to the voters of `ids` and `idsj` in order,
/// each voter once, with the zeros that stand for `_` left out: no entry is not
/// the same as a zero entry.
fn lookuper(
  input : Array[UInt64],
  ids : Array[String],
  idsj : Array[String],
) -> Map[String, UInt64] {
  let l : Map[String, UInt64] = Map([])
  let mut p = 0
  for id in [..ids, ..idsj] {
    if l.contains(id) {
      continue
    }
    if p < input.length() {
      l[id] = input[p]
      p += 1
    }
  }
  for id in l.keys().to_array() {
    if l[id] == 0 {
      l.remove(id)
    }
  }
  l
}

///|
/// etcd's `alternativeMajorityCommittedIndex`: the largest index a quorum has
/// acked, found by counting rather than sorting.
fn alternative_committed(c : Array[String], l : Map[String, UInt64]) -> UInt64 {
  if c.is_empty() {
    return 18446744073709551615UL
  }
  let acked : Array[UInt64] = []
  for id in c {
    if l.get(id) is Some(idx) {
      acked.push(idx)
    }
  }
  let q = c.length() / 2 + 1
  let mut best = 0UL
  for idy in acked {
    let votes = acked.filter(idx => idx >= idy).length()
    if votes >= q && idy > best {
      best = idy
    }
  }
  best
}

///|
fn vote_text(v : @quorum.VoteState) -> String {
  match v {
    VoteWon => "VoteWon"
    VoteLost => "VoteLost"
    VotePending => "VotePending"
  }
}

///|
/// The quorum datadriven harness, after etcd's `quorum/datadriven_test.go`.
pub fn run_quorum(line : String, _input : String) -> String {
  let a = parse_args(line)
  let ids = union_ids(parse_list(kv_get(a.kv, "cfg", "")), [])
  let joint = a.kv.contains("cfgj")
  let idsj = union_ids(parse_list(kv_get(a.kv, "cfgj", "")), [])
  let idxs = parse_list(kv_get(a.kv, "idx", "")).map(v => {
    if v == "_" {
      0UL
    } else {
      parse_u64(v)
    }
  })
  let votes = parse_list(kv_get(a.kv, "votes", "")).map(v => {
    match v {
      "y" => 2UL
      "n" => 1UL
      _ => 0UL
    }
  })
  let input = if a.cmd == "vote" { votes } else { idxs }
  let voters = union_ids(ids, idsj)
  if voters.length() != input.length() {
    let shown = input.map(index_text).join(" ")
    return "error: mismatched input (explicit or _) for voters [\{voters.join(" ")}]: [\{shown}]"
  }
  let buf = StringBuilder()
  match a.cmd {
    "committed" => {
      let l = lookuper(idxs, ids, idsj)
      if !joint {
        let idx = @quorum.committed_index(ids, [], l)
        buf.write_string(@quorum.describe(ids, l))
        let alt = alternative_committed(ids, l)
        if alt != idx {
          buf.write_string(
            "\{index_text(alt)} <-- via alternative computation\n",
          )
        }
        let zero = @quorum.committed_index(ids, [], l)
        if zero != idx {
          buf.write_string("\{index_text(zero)} <-- via zero-joint quorum\n")
        }
        let own = @quorum.committed_index(ids, ids, l)
        if own != idx {
          buf.write_string("\{index_text(own)} <-- via self-joint quorum\n")
        }
        for id in ids {
          let iidx = l.get(id).unwrap_or(0)
          if idx > iidx && iidx > 0 {
            for lowered in [iidx - 1, 0UL] {
              let lo : Map[String, UInt64] = Map([])
              for iid in ids {
                if iid == id {
                  lo[iid] = lowered
                } else if l.get(iid) is Some(v) {
                  lo[iid] = v
                }
              }
              let got = @quorum.committed_index(ids, [], lo)
              if got != idx {
                let shown = if lowered == 0 { "0" } else { iidx.to_string() }
                buf.write_string(
                  "\{index_text(got)} <-- overlaying \{id}->\{shown}",
                )
              }
            }
          }
        }
        buf.write_string(index_text(idx) + "\n")
      } else {
        buf.write_string(@quorum.describe(voters, l))
        let idx = @quorum.committed_index(ids, idsj, l)
        let swapped = @quorum.committed_index(idsj, ids, l)
        if swapped != idx {
          buf.write_string("\{index_text(swapped)} <-- via symmetry\n")
        }
        buf.write_string(index_text(idx) + "\n")
      }
    }
    "vote" => {
      let ll = lookuper(votes, ids, idsj)
      let l : Map[String, Bool] = Map([])
      for id, v in ll {
        l[id] = v != 1
      }
      if !joint {
        buf.write_string(vote_text(@quorum.vote_result(ids, [], l)) + "\n")
      } else {
        let r = @quorum.vote_result(ids, idsj, l)
        let swapped = @quorum.vote_result(idsj, ids, l)
        if swapped != r {
          buf.write_string(vote_text(swapped) + " <-- via symmetry\n")
        }
        buf.write_string(vote_text(r) + "\n")
      }
    }
    _ => return "unknown command: \{a.cmd}"
  }
  buf.to_string()
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
        'u' => "u"
        _ => "?"
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
  // etcd's harness moves the changer's last index on after every command.
  defer self.changer.advance_index()
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
    Some(e) => e
    None => self.changer.describe().trim_end().to_owned()
  }
}
