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
/// The smallest useful tour of the library: elect a leader, replicate a
/// command, kill the leader, and confirm the survivors still agree. The
/// simulation and its assertions live in `@raft.demo_run` / `demo_report_lines`
/// so they can be tested; this entry point only performs the output.
///
/// Run it with `moon run cmd/example`.
#coverage.skip
fn main {
  for
    line in @raft.demo_report_lines(
      @raft.demo_run(["a", "b", "c", "d", "e"], 1, 200, 200, 400),
    ) {
    println(line)
  }
}
