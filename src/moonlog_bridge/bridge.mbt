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
// The bridge from moonraft's logger seam to moonlog. The core only knows
// `core.Logger` — one `log(level, message)` method, no backend, no clock — so a
// deployment that already runs moonlog plugs it in here rather than teaching the
// consensus engine a second logging model.

///|
/// A `core.Logger` that forwards every record to a moonlog logger.
///
/// The two seams line up one-for-one: moonlog's `Sink` is where a record goes
/// and `Logger` is a level filter in front of it, which is the same split
/// moonraft makes between the `Logger` trait and the `NopLogger` default. Only
/// the level names differ, and they differ by one: moonlog has `Trace`, which
/// the consensus core never logs at, and accepts `Fatal` for etcd's `Panic`.
pub struct MoonlogLogger {
  inner : @moonlog.Logger
}

///|
/// Wrap a moonlog logger (see `@moonlog.Logger::new`).
pub fn MoonlogLogger::new(inner : @moonlog.Logger) -> MoonlogLogger {
  { inner, }
}

///|
/// A bridge writing to `sink` at `level`, so a caller that already has a moonlog
/// sink does not have to build the logger itself.
pub fn MoonlogLogger::to_sink(
  sink : &@moonlog.Sink,
  level~ : @moonlog.Level = @moonlog.level,
) -> MoonlogLogger {
  { inner: @moonlog.Logger::new(sink, level~), }
}

///|
/// The wrapped moonlog logger, for a caller that wants to keep logging through it
/// directly between consensus records.
pub fn MoonlogLogger::inner(self : MoonlogLogger) -> @moonlog.Logger {
  self.inner
}

///|
/// This bridge as the consensus core sees it: a `core.Logger` to hand to
/// `RaftNode::new` / `Config`.
pub fn MoonlogLogger::as_logger(self : MoonlogLogger) -> &@core.Logger {
  self
}

///|
/// Translate a consensus level to a moonlog one.
///
/// `Panic` maps to `Fatal` rather than to an abort: etcd's `Panicf` logs and
/// then panics, and the panic is the caller's to raise — moonlog deliberately
/// has no level that kills the process, so neither does the bridge.
fn to_moonlog(level : @core.LogLevel) -> @moonlog.Level {
  match level {
    Debug => @moonlog.Debug
    Info => @moonlog.Info
    Warning => @moonlog.Warning
    Error => @moonlog.Error
    Fatal => @moonlog.Fatal
    Panic => @moonlog.Fatal
  }
}

///|
impl @core.Logger for MoonlogLogger with fn log(self, level, message) {
  let mapped = to_moonlog(level)
  // Ask first: a filtered-out record should cost nothing, which is what
  // moonlog's `enabled` exists for.
  if self.inner.enabled(mapped) {
    self.inner.log(mapped, message)
  }
}
