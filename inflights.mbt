// Ported from etcd-io/raft (Apache-2.0): tracker/inflights.go.
// See PORTING.md for the source-test correspondence.

///|
/// One in-flight AppendEntries message: the index of its last entry and the
/// total byte size of the entries it carries.
pub(all) struct Inflight {
  mut index : UInt64
  mut bytes : UInt64
} derive(Eq)

///|
/// A sliding-window flow controller for the AppendEntries messages a leader has
/// sent to one follower but not yet had acknowledged. It caps both the number
/// of outstanding messages (`size`) and their total byte size (`max_bytes`),
/// which is what stops a leader from flooding a lagging follower. Callers check
/// `full` before sending, `add` on each send, and `free_le` on each ack.
pub struct Inflights {
  mut start : Int
  mut count : Int
  mut bytes : UInt64
  size : Int
  max_bytes : UInt64
  mut buffer : Array[Inflight]
}

///|
fn zero_inflight() -> Inflight {
  { index: 0, bytes: 0 }
}

///|
/// A tracker allowing up to `size` in-flight messages and up to `max_bytes`
/// total bytes. `max_bytes` of 0 means no byte limit. The byte limit is soft:
/// one message that crosses it is still accepted.
pub fn Inflights::new(size : Int, max_bytes : UInt64) -> Inflights {
  { start: 0, count: 0, bytes: 0, size, max_bytes, buffer: [] }
}

///|
/// A deep copy that shares no buffer memory with the receiver.
pub fn Inflights::clone(self : Inflights) -> Inflights {
  {
    start: self.start,
    count: self.count,
    bytes: self.bytes,
    size: self.size,
    max_bytes: self.max_bytes,
    buffer: Array::makei(self.buffer.length(), fn(i) {
      { index: self.buffer[i].index, bytes: self.buffer[i].bytes }
    }),
  }
}

///|
/// Whether no more messages may be sent right now: the message count is at its
/// cap, or the byte budget is exhausted.
pub fn Inflights::full(self : Inflights) -> Bool {
  self.count == self.size ||
  (self.max_bytes != 0 && self.bytes >= self.max_bytes)
}

///|
/// The number of in-flight messages.
pub fn Inflights::count(self : Inflights) -> Int {
  self.count
}

///|
/// Record that a message ending at `index` and carrying `bytes` bytes has been
/// dispatched. `full` must be false first, and consecutive calls must pass a
/// monotonic sequence of indexes.
pub fn Inflights::add(self : Inflights, index : UInt64, bytes : UInt64) -> Unit {
  if self.full() {
    abort("cannot add into a Full inflights")
  }
  let mut next = self.start + self.count
  if next >= self.size {
    next = next - self.size
  }
  if next >= self.buffer.length() {
    self.grow()
  }
  self.buffer[next] = { index, bytes }
  self.count = self.count + 1
  self.bytes = self.bytes + bytes
}

///|
/// Double the ring buffer on demand, never past `size`. Growing lazily keeps a
/// process that hosts thousands of Raft groups from pre-allocating every window.
fn Inflights::grow(self : Inflights) -> Unit {
  let mut new_size = self.buffer.length() * 2
  if new_size == 0 {
    new_size = 1
  } else if new_size > self.size {
    new_size = self.size
  }
  let old = self.buffer
  self.buffer = Array::makei(new_size, fn(i) {
    if i < old.length() {
      old[i]
    } else {
      zero_inflight()
    }
  })
}

///|
/// Free every in-flight message with last index at or below `to`, releasing its
/// quota. Acks out of the left edge of the window are ignored.
pub fn Inflights::free_le(self : Inflights, to : UInt64) -> Unit {
  if self.count == 0 || to < self.buffer[self.start].index {
    return
  }
  let mut idx = self.start
  let mut i = 0
  let mut freed_bytes = 0UL
  while i < self.count {
    if to < self.buffer[idx].index {
      break
    }
    freed_bytes = freed_bytes + self.buffer[idx].bytes
    idx = idx + 1
    if idx >= self.size {
      idx = idx - self.size
    }
    i = i + 1
  }
  self.count = self.count - i
  self.bytes = self.bytes - freed_bytes
  self.start = idx
  if self.count == 0 {
    self.start = 0
  }
}

///|
/// Free all in-flight messages, e.g. when a follower's progress is reset.
pub fn Inflights::reset(self : Inflights) -> Unit {
  self.start = 0
  self.count = 0
  self.bytes = 0
}
