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
/// A severity level, ordered from most to least verbose. Mirrors the five
/// levels etcd's `Logger` exposes; `Panic` additionally aborts.
pub enum LogLevel {
  Debug
  Info
  Warning
  Error
  Fatal
  Panic
} derive(Eq)

///|
/// A pluggable sink for the consensus core's diagnostics (etcd's `raft.Logger`).
/// Where etcd's Go interface exposes twelve `printf`-style methods (a `Debug`/
/// `Debugf` pair per level), a MoonBit caller formats the message with string
/// interpolation at the call site, so a single `log(level, message)` carries
/// every level and the `LogLevel` enum supplies the level distinction. Keeping
/// it a trait decouples the core from any particular logging backend, and the
/// no-op default (`NopLogger`) keeps a server that wants no logging
/// allocation-free.
pub trait Logger {
  fn log(Self, LogLevel, String) -> Unit
}

///|
/// The default `Logger`: it discards every message (etcd defaults to a real
/// stderr logger, but a deterministic simulation and its tests want silence).
/// A deployment that wants diagnostics supplies its own `Logger`.
pub struct NopLogger {}

///|
impl Logger for NopLogger with fn log(_self, _level, _msg) {

}
