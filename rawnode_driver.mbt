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

///|
/// Run a single-node Raft group through the Ready/Advance loop over a batch of
/// client commands, returning the commands the state machine applied, in order.
///
/// This is the smallest complete embedding of a `RawNode`, and it is the live
/// documentation of the Ready contract: for each command, propose it, then drain
/// every `Ready` by running the three steps a real application runs, in order —
/// **persist** the `entries` (`store`), **apply** the `committed_entries`, then
/// **advance**. Because persistence happens before application, `committed_entries`
/// may cover the same freshly appended entries as `entries` in one `Ready`, just
/// as in etcd. It is the non-test entry point that wires `RawNode` into a runnable
/// driver, and a demo or an embedder can call it directly.
pub fn run_single_node(id : String, commands : Array[Bytes]) -> Array[Bytes] {
  let raw = RaftNode::new(id, []).raw()
  raw.campaign()
  raw.stabilize() |> ignore
  let applied : Array[Bytes] = []
  for cmd in commands {
    raw.propose(cmd)
    while raw.has_ready() {
      let rd = raw.ready()
      raw.store(rd) // persist entries to stable storage first
      for e in rd.committed_entries {
        applied.push(e.command) // then apply committed entries
      }
      raw.advance(rd) // then advance the cursors
    }
  }
  applied
}
