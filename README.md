<div align="center">

# raft-moonbit — porting report & differential testing

**`difftest` branch** — the full consensus source plus the **Go-versus-MoonBit differential-test harness**, the porting report, and the coverage evidence.

The library and its docs live on **[`master`](https://github.com/Lfan-ke/raft-moonbit/tree/master)**; everything on this branch is the *proof of fidelity*.

[![tests](https://img.shields.io/badge/tests-723%20passing-2ea44f)](#tests--coverage)
[![coverage](https://img.shields.io/badge/coverage-100%25%20line%20%26%20branch-2ea44f)](#tests--coverage)
[![porting](https://img.shields.io/badge/PORTING-0%20partial%20%2F%200%20todo-2ea44f)](#porting-status)
[![upstream](https://img.shields.io/badge/upstream-etcd--io%2Fraft%4026647d5-blue)](https://github.com/etcd-io/raft)

</div>

## Porting status

A faithful, **line-by-line** port of the standalone Go library **`etcd-io/raft@26647d5`** (the raft library, *not* the etcd database).

- The census in [`PORTING.md`](PORTING.md) tracks every upstream `Test*` function. It has **no `PARTIAL` / `TODO` / `PORT-pending` rows left** — every test that does not depend on Go's runtime is ported *assertion-for-assertion* (no simplified cases, no skipped table rows, no weakened assertions). Each remaining **`N/A`** is one of three acceptable kinds — a Go-concurrency shell (goroutines/channels), a benchmark, or a Go struct-memory-layout assert — and names its MoonBit equivalent.
- All **258** upstream `Test*` functions have a counterpart.
- Verified three independent ways (transliteration · adversarial audit · differential trace), which together surfaced and fixed **24 correctness defects** (safety / liveness / behavioural / accounting) plus 2 default-config mismatches — each under a red-then-green regression test still in the suite.

## Tests & coverage

```bash
moon check --deny-warn --target all       # 0 warnings on wasm / wasm-gc / js / native
moon test                                 # Total tests: 723, passed: 723, failed: 0
moon clean && moon test --enable-coverage
moon coverage analyze                     # All source files are fully covered
moon coverage report -f summary           # Total: 3094/3094
```

Embedded coverage report:

```
$ moon coverage analyze
All source files are fully covered

$ moon coverage report -f summary
Total: 3094/3094
```

Coverage has **two denominators, both 100%**: `analyze` counts uncovered **source lines**; `summary` counts **coverage points** — each `if`/`else` arm, each `||`/`&&` operand, and each `match` arm counts as one point. 100% line coverage does *not* imply 100% branch coverage; both are achieved. **CI enforces both** — the build fails if either regresses.

A **browsable per-file HTML report** (every `.mbt` at 100%) is published at **https://lfan-ke.github.io/raft-moonbit/coverage/**, and a Cobertura XML (`line-rate="1.0"`) can be regenerated with `moon coverage report -f cobertura`.

`abort`-expressed panic contracts (illegal `Config`, out-of-bounds `commit_to`, etc.) are covered with MoonBit's panic tests (a test named `panic …` must abort or it fails), the same facility `moonbitlang/core` uses.

## Differential test — Go vs MoonBit, event by event

The harness drives the **same scenario** through etcd's real `RawNode` (Go) and this port (MoonBit), then compares their state traces field-by-field. Upstream is pinned as a git submodule, and exactly **one** file is overlaid onto it.

```bash
# build both harnesses, run every scenario, print a MATCH/DIVERGE summary
bash difftest/run.sh

# run a single scenario and print its per-event diff
bash difftest/run.sh difftest/scripts/13_reordered_reject.script
```

| Path | Role |
| --- | --- |
| `difftest/vendor/etcd-raft` | pristine `etcd-io/raft@26647d5` (git submodule) |
| `difftest/harness_export.go` | the **only** file overlaid onto the pinned checkout |
| `difftest/go-harness` | Go driver over etcd `RawNode` → normalized JSON trace |
| `difftest/mbt-harness` | MoonBit driver (imports the public `@raft.*` facade) → same schema |
| `difftest/scripts/*.script` | scenario DSL (election, pre-vote, partition, crash, log conflict, flow control, membership, learner, ReadIndex, leadership transfer, stale message, reordered reject, single-voter read) |
| `difftest/diff.py` | per-event comparator |

**Result** ([`difftest/DIFF_REPORT.md`](difftest/DIFF_REPORT.md)): 14 scenarios (12 + 2 positive controls), **no `MATCH → DIVERGE` regression**. The residual divergences are representation artifacts (heartbeat `Map` iteration order, conf-change append shape), not behaviour. The `reject_index` fix is **positively reproduced**: reverting it makes MoonBit emit one spurious probe and the trace diverges; restoring it returns to a frame-identical `MATCH`.

Requirements: Go (`GOTOOLCHAIN=auto`), the MoonBit toolchain, and `python`. `run.sh` overlays `harness_export.go`, builds the Go harness, generates each scenario's MoonBit driver, and diffs the two traces.

## Quick example (the library)

```moonbit
let cluster = @raft.Cluster::new(["a", "b", "c", "d", "e"], seed=1)
let leader = cluster.run_until_leader(200)          // elect a leader
let _ = cluster.propose(b"set x = 1")               // replicate a command
let _ = cluster.run_until_committed(2, 200)         // wait for commit

cluster.crash(leader.unwrap())                       // inject a fault
let _ = cluster.run_until_leader(400)               // a new leader takes over
assert_true(cluster.one_leader_per_term())          // safety still holds
assert_true(cluster.committed_agrees())
```

## The gh-pages site

The `docs/` folder is a static site plus a **WebAssembly** demo (five nodes, five Web Workers, real consensus in the browser). Build and serve locally:

```bash
moon build --target wasm --release              # -> _build/wasm/release/build/demo/demo.wasm
cp _build/wasm/release/build/demo/demo.wasm docs/raft-moonbit.wasm
python3 -m http.server 8099 --directory docs    # then open http://localhost:8099/
```

Workers `fetch` the wasm, so a `file://` URL will not work. Live:

- **Home** — https://lfan-ke.github.io/raft-moonbit/
- **Live demo** — https://lfan-ke.github.io/raft-moonbit/demo.html
- **API reference** — https://lfan-ke.github.io/raft-moonbit/api.html

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). A MoonBit port of [etcd-io/raft](https://github.com/etcd-io/raft) (Copyright 2015 The etcd Authors). Mirrored on [GitLink](https://gitlink.org.cn/heke1228/raft-moonbit) — same author, same history.
