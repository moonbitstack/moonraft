function _M0TP28heke122814raft_2dmoonbit9DemoState(param0, param1) {
  this.cluster = param0;
  this.seq = param1;
}
function _M0TPB13StringBuilder(param0) {
  this.val = param0;
}
const _M0FPB12random__seed = () => {
  if (globalThis.crypto?.getRandomValues) {
    const array = new Uint32Array(1);
    globalThis.crypto.getRandomValues(array);
    return array[0] | 0; // Convert to signed 32
  } else {
    return Math.floor(Math.random() * 0x100000000) | 0; // Fallback to Math.random
  }
};
const _M0FPB19int__to__string__js = (x, radix) => {
  return x.toString(radix);
};
const _M0FPB22uint64__to__string__js = (num, radix) => num.toString(radix);
function _M0TPB4IterGUsRP28heke122814raft_2dmoonbit8ProgressEE(param0, param1) {
  this.f = param0;
  this.size_hint = param1;
}
const _M0MPB7JSArray4push = (arr, val) => { arr.push(val); };
class $PanicError extends Error {}
function $panic() {
  throw new $PanicError();
}
function $make_array_len_and_init(a, b) {
  const arr = new Array(a);
  arr.fill(b);
  return arr;
}
function _M0TPB3MapGsRP28heke122814raft_2dmoonbit8RaftNodeE(param0, param1, param2, param3, param4, param5, param6) {
  this.entries = param0;
  this.size = param1;
  this.capacity = param2;
  this.capacity_mask = param3;
  this.grow_at = param4;
  this.head = param5;
  this.tail = param6;
}
function _M0TPB3MapGsiE(param0, param1, param2, param3, param4, param5, param6) {
  this.entries = param0;
  this.size = param1;
  this.capacity = param2;
  this.capacity_mask = param3;
  this.grow_at = param4;
  this.head = param5;
  this.tail = param6;
}
function _M0TPB3MapGsbE(param0, param1, param2, param3, param4, param5, param6) {
  this.entries = param0;
  this.size = param1;
  this.capacity = param2;
  this.capacity_mask = param3;
  this.grow_at = param4;
  this.head = param5;
  this.tail = param6;
}
function $bound_check(arr, index) {
  if (index < 0 || index >= arr.length) throw new Error("Index out of bounds");
}
function _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8RaftNodeE(param0, param1, param2, param3, param4, param5) {
  this.prev = param0;
  this.next = param1;
  this.psl = param2;
  this.hash = param3;
  this.key = param4;
  this.value = param5;
}
function _M0TPB5EntryGsiE(param0, param1, param2, param3, param4, param5) {
  this.prev = param0;
  this.next = param1;
  this.psl = param2;
  this.hash = param3;
  this.key = param4;
  this.value = param5;
}
function _M0TPB5EntryGsbE(param0, param1, param2, param3, param4, param5) {
  this.prev = param0;
  this.next = param1;
  this.psl = param2;
  this.hash = param3;
  this.key = param4;
  this.value = param5;
}
function _M0TPB5EntryGmsE(param0, param1, param2, param3, param4, param5) {
  this.prev = param0;
  this.next = param1;
  this.psl = param2;
  this.hash = param3;
  this.key = param4;
  this.value = param5;
}
function _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8ProgressE(param0, param1, param2, param3, param4, param5) {
  this.prev = param0;
  this.next = param1;
  this.psl = param2;
  this.hash = param3;
  this.key = param4;
  this.value = param5;
}
function _M0TPB8MutLocalGORPB5EntryGsRP28heke122814raft_2dmoonbit8ProgressEE(param0) {
  this.val = param0;
}
function _M0TPB8MutLocalGiE(param0) {
  this.val = param0;
}
function _M0TPB8MutLocalGORPB5EntryGsbEE(param0) {
  this.val = param0;
}
const _M0MPB7JSArray4copy = (arr) => arr.slice(0);
const $bytes_literal$0 = new Uint8Array();
function $makebytes(a, b) {
  const arr = new Uint8Array(a);
  if (b !== 0) {
    arr.fill(b);
  }
  return arr;
}
const _M0MPB7JSArray11set__length = (arr, len) => { arr.length = len; };
const _M0MPB7JSArray3pop = (arr) => arr.pop();
function _M0TP28heke122814raft_2dmoonbit5Entry(param0, param1, param2, param3) {
  this.term = param0;
  this.index = param1;
  this.entry_type = param2;
  this.command = param3;
}
function _M0TP28heke122814raft_2dmoonbit10RaftStatus(param0, param1, param2, param3, param4, param5, param6) {
  this.id = param0;
  this.role = param1;
  this.term = param2;
  this.leader = param3;
  this.commit = param4;
  this.last_index = param5;
  this.applied = param6;
}
function _M0TP28heke122814raft_2dmoonbit4Node(param0, param1, param2, param3, param4, param5, param6, param7, param8) {
  this.id = param0;
  this.role = param1;
  this.current_term = param2;
  this.voted_for = param3;
  this.log = param4;
  this.commit_index = param5;
  this.last_applied = param6;
  this.snapshot_index = param7;
  this.snapshot_term = param8;
}
function _M0TP28heke122814raft_2dmoonbit19InstallSnapshotArgs(param0, param1, param2, param3, param4, param5, param6) {
  this.term = param0;
  this.leader_id = param1;
  this.last_index = param2;
  this.last_term = param3;
  this.offset = param4;
  this.data = param5;
  this.done = param6;
}
function _M0TP28heke122814raft_2dmoonbit20InstallSnapshotReply(param0) {
  this.term = param0;
}
function _M0TPB9ArrayViewGUmsEE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
function $bytes_equal(a, b) {
    if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}
function _M0TP28heke122814raft_2dmoonbit10Membership(param0, param1, param2) {
  this.members = param0;
  this.outgoing = param1;
  this.joint = param2;
}
function _M0TP28heke122814raft_2dmoonbit8RaftNode(param0, param1, param2, param3, param4, param5, param6, param7, param8, param9, param10, param11, param12, param13, param14, param15, param16) {
  this.core = param0;
  this.id = param1;
  this.peers = param2;
  this.config = param3;
  this.progress = param4;
  this.votes = param5;
  this.pre_vote = param6;
  this.check_quorum = param7;
  this.in_pre_campaign = param8;
  this.leader_id = param9;
  this.election_elapsed = param10;
  this.heartbeat_elapsed = param11;
  this.election_timeout = param12;
  this.heartbeat_timeout = param13;
  this.randomized_election_timeout = param14;
  this.max_entries = param15;
  this.rng = param16;
}
function _M0TPB9ArrayViewGUsRP28heke122814raft_2dmoonbit8ProgressEE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
function _M0TPB9ArrayViewGUsbEE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
function _M0TPB9ArrayViewGUsRP28heke122814raft_2dmoonbit8RaftNodeEE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
function _M0TPB9ArrayViewGUsiEE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
function _M0TP28heke122814raft_2dmoonbit7Cluster(param0, param1, param2, param3, param4, param5, param6, param7, param8, param9) {
  this.nodes = param0;
  this.ids = param1;
  this.inflight = param2;
  this.now = param3;
  this.part = param4;
  this.down = param5;
  this.drop_permil = param6;
  this.max_delay = param7;
  this.next_group = param8;
  this.rng = param9;
}
function _M0TP28heke122814raft_2dmoonbit8InFlight(param0, param1) {
  this.deliver_at = param0;
  this.msg = param1;
}
function _M0TP28heke122814raft_2dmoonbit7Message(param0, param1, param2) {
  this.from = param0;
  this.to = param1;
  this.payload = param2;
}
function _M0TP28heke122814raft_2dmoonbit18AppendEntriesReply(param0, param1, param2, param3) {
  this.term = param0;
  this.success = param1;
  this.match_index = param2;
  this.conflict_index = param3;
}
function _M0DTP28heke122814raft_2dmoonbit7Payload7PreVote(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload7PreVote.prototype.$tag = 0;
function _M0DTP28heke122814raft_2dmoonbit7Payload11PreVoteResp(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload11PreVoteResp.prototype.$tag = 1;
function _M0DTP28heke122814raft_2dmoonbit7Payload4Vote(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload4Vote.prototype.$tag = 2;
function _M0DTP28heke122814raft_2dmoonbit7Payload8VoteResp(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload8VoteResp.prototype.$tag = 3;
function _M0DTP28heke122814raft_2dmoonbit7Payload6Append(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload6Append.prototype.$tag = 4;
function _M0DTP28heke122814raft_2dmoonbit7Payload10AppendResp(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload10AppendResp.prototype.$tag = 5;
function _M0DTP28heke122814raft_2dmoonbit7Payload8Snapshot(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload8Snapshot.prototype.$tag = 6;
function _M0DTP28heke122814raft_2dmoonbit7Payload12SnapshotResp(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload12SnapshotResp.prototype.$tag = 7;
function _M0DTP28heke122814raft_2dmoonbit7Payload10TimeoutNow(param0) {
  this._0 = param0;
}
_M0DTP28heke122814raft_2dmoonbit7Payload10TimeoutNow.prototype.$tag = 8;
function _M0TP28heke122814raft_2dmoonbit8Snapshot(param0, param1, param2) {
  this.last_index = param0;
  this.last_term = param1;
  this.data = param2;
}
function _M0TP28heke122814raft_2dmoonbit17AppendEntriesArgs(param0, param1, param2, param3, param4, param5) {
  this.term = param0;
  this.leader_id = param1;
  this.prev_log_index = param2;
  this.prev_log_term = param3;
  this.entries = param4;
  this.leader_commit = param5;
}
function _M0TP28heke122814raft_2dmoonbit16RequestVoteReply(param0, param1) {
  this.term = param0;
  this.vote_granted = param1;
}
function _M0TP28heke122814raft_2dmoonbit15RequestVoteArgs(param0, param1, param2, param3) {
  this.term = param0;
  this.candidate_id = param1;
  this.last_log_index = param2;
  this.last_log_term = param3;
}
function _M0TP28heke122814raft_2dmoonbit8Progress(param0, param1, param2, param3) {
  this.next_index = param0;
  this.match_index = param1;
  this.state = param2;
  this.recent_active = param3;
}
function _M0TPB9ArrayViewGyE(param0, param1, param2) {
  this.buf = param0;
  this.start = param1;
  this.end = param2;
}
const _M0MPB4Iter4nextN6constrS9224GUsRP28heke122814raft_2dmoonbit8ProgressEE = 0;
const _M0MPB4Iter4nextN6constrS9225GUsRP28heke122814raft_2dmoonbit8ProgressEE = 0;
const _M0MPB4Iter3newN6constrS9232GUsRP28heke122814raft_2dmoonbit8ProgressEE = 0;
const _M0FP28heke122814raft_2dmoonbit9demo__ctx = new _M0TP28heke122814raft_2dmoonbit9DemoState(undefined, 0);
const _M0FPB4seed = _M0FPB12random__seed();
const _M0MP28heke122814raft_2dmoonbit7Cluster9reachableN6constrS1522 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster9reachableN6constrS1523 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster4tickN6constrS1524 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster7leadersN6constrS1525 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster11min__commitN6constrS1516 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agreesN6constrS1517 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agreesN6constrS1518 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster25same__committed__commandsN6constrS1520 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster8is__downN6constrS1521 = true;
const _M0MP28heke122814raft_2dmoonbit7Cluster16logs__consistentN6constrS1519 = true;
function _M0FPB13consume4__acc(acc, input) {
  const _p = (acc >>> 0) + ((Math.imul(input, -1028477379) | 0) >>> 0) | 0;
  const _p$2 = 17;
  return Math.imul(_p << _p$2 | (_p >>> (32 - _p$2 | 0) | 0), 668265263) | 0;
}
function _M0MPB13StringBuilder21StringBuilder_2einner(size_hint) {
  return new _M0TPB13StringBuilder("");
}
function _M0IPB13StringBuilderPB6Logger11write__char(self, ch) {
  self.val = `${self.val}${String.fromCodePoint(ch)}`;
}
function _M0IPB13StringBuilderPB6Logger13write__string(self, str) {
  self.val = `${self.val}${str}`;
}
function _M0IP016_24default__implPB2Eq10not__equalGObE(x, y) {
  return !_M0IPC16option6OptionPB2Eq5equalGbE(x, y);
}
function _M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(x, y) {
  return !_M0IP28heke122814raft_2dmoonbit4RolePB2Eq5equal(x, y);
}
function _M0FPB14avalanche__acc(acc) {
  let acc$2 = acc;
  acc$2 = acc$2 ^ (acc$2 >>> 15 | 0);
  acc$2 = Math.imul(acc$2, -2048144777) | 0;
  acc$2 = acc$2 ^ (acc$2 >>> 13 | 0);
  acc$2 = Math.imul(acc$2, -1028477379) | 0;
  acc$2 = acc$2 ^ (acc$2 >>> 16 | 0);
  return acc$2;
}
function _M0FPB13finalize__acc(acc) {
  return _M0FPB14avalanche__acc(acc);
}
function _M0MPB4Iter4nextGUsRP28heke122814raft_2dmoonbit8ProgressEE(self) {
  const _func = self.f;
  const result = _func();
  const _bind = self.size_hint;
  if (result === undefined) {
    self.size_hint = _M0MPB4Iter4nextN6constrS9225GUsRP28heke122814raft_2dmoonbit8ProgressEE;
  } else {
    if (_bind === undefined) {
    } else {
      const _Some = _bind;
      const _n = _Some;
      self.size_hint = _n > 0 ? _n - 1 | 0 : _M0MPB4Iter4nextN6constrS9224GUsRP28heke122814raft_2dmoonbit8ProgressEE;
    }
  }
  return result;
}
function _M0MPC13int3Int18to__string_2einner(self, radix) {
  return _M0FPB19int__to__string__js(self, radix);
}
function _M0MPC16uint646UInt6418to__string_2einner(self, radix) {
  return _M0FPB22uint64__to__string__js(self, radix);
}
function _M0MPB4Iter3newGUsRP28heke122814raft_2dmoonbit8ProgressEE(f, size_hint) {
  let size_hint$2;
  if (size_hint === undefined) {
    size_hint$2 = undefined;
  } else {
    const _Some = size_hint;
    const _n = _Some;
    size_hint$2 = _n > 0 ? _n : _M0MPB4Iter3newN6constrS9232GUsRP28heke122814raft_2dmoonbit8ProgressEE;
  }
  return new _M0TPB4IterGUsRP28heke122814raft_2dmoonbit8ProgressEE(f, size_hint$2);
}
function _M0MPC15array5Array4pushGsE(self, value) {
  _M0MPB7JSArray4push(self, value);
}
function _M0MPC15array5Array4pushGyE(self, value) {
  _M0MPB7JSArray4push(self, value);
}
function _M0IPC14char4CharPB4Show10to__string(self) {
  return String.fromCodePoint(self);
}
function _M0IPC16option6OptionPB2Eq5equalGbE(self, other) {
  if (self === -1) {
    return other === -1;
  } else {
    const _Some = self;
    const _x = _Some;
    if (other === -1) {
      return false;
    } else {
      const _Some$2 = other;
      const _y = _Some$2;
      return _x === _y;
    }
  }
}
function _M0MPC13int3Int20next__power__of__two(self) {
  if (self >= 0) {
    if (self <= 1) {
      return 1;
    }
    if (self > 1073741824) {
      return 1073741824;
    }
    return (2147483647 >> (Math.clz32(self - 1 | 0) - 1 | 0)) + 1 | 0;
  } else {
    return $panic();
  }
}
function _M0FPB8new__mapGsRP28heke122814raft_2dmoonbit8RaftNodeE(capacity) {
  const capacity$2 = _M0MPC13int3Int20next__power__of__two(capacity);
  const _bind = capacity$2 - 1 | 0;
  const _bind$2 = (Math.imul(capacity$2, 13) | 0) / 16 | 0;
  const _bind$3 = $make_array_len_and_init(capacity$2, undefined);
  const _bind$4 = undefined;
  return new _M0TPB3MapGsRP28heke122814raft_2dmoonbit8RaftNodeE(_bind$3, 0, capacity$2, _bind, _bind$2, _bind$4, -1);
}
function _M0FPB8new__mapGsiE(capacity) {
  const capacity$2 = _M0MPC13int3Int20next__power__of__two(capacity);
  const _bind = capacity$2 - 1 | 0;
  const _bind$2 = (Math.imul(capacity$2, 13) | 0) / 16 | 0;
  const _bind$3 = $make_array_len_and_init(capacity$2, undefined);
  const _bind$4 = undefined;
  return new _M0TPB3MapGsiE(_bind$3, 0, capacity$2, _bind, _bind$2, _bind$4, -1);
}
function _M0FPB8new__mapGsbE(capacity) {
  const capacity$2 = _M0MPC13int3Int20next__power__of__two(capacity);
  const _bind = capacity$2 - 1 | 0;
  const _bind$2 = (Math.imul(capacity$2, 13) | 0) / 16 | 0;
  const _bind$3 = $make_array_len_and_init(capacity$2, undefined);
  const _bind$4 = undefined;
  return new _M0TPB3MapGsbE(_bind$3, 0, capacity$2, _bind, _bind$2, _bind$4, -1);
}
function _M0FPB21capacity__for__length(length) {
  let capacity = _M0MPC13int3Int20next__power__of__two(length);
  const _p = capacity;
  if (length > ((Math.imul(_p, 13) | 0) / 16 | 0)) {
    capacity = Math.imul(capacity, 2) | 0;
  }
  return capacity;
}
function _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry) {
  const _bind = self.tail;
  if (_bind === -1) {
    self.head = entry;
  } else {
    const _tmp = self.entries;
    $bound_check(_tmp, _bind);
    const _p = _tmp[_bind];
    let _tmp$2;
    if (_p === undefined) {
      _tmp$2 = $panic();
    } else {
      const _p$2 = _p;
      _tmp$2 = _p$2;
    }
    _tmp$2.next = entry;
  }
  self.tail = idx;
  const _tmp = self.entries;
  $bound_check(_tmp, idx);
  _tmp[idx] = entry;
  self.size = self.size + 1 | 0;
}
function _M0MPB3Map20add__entry__to__tailGsiE(self, idx, entry) {
  const _bind = self.tail;
  if (_bind === -1) {
    self.head = entry;
  } else {
    const _tmp = self.entries;
    $bound_check(_tmp, _bind);
    const _p = _tmp[_bind];
    let _tmp$2;
    if (_p === undefined) {
      _tmp$2 = $panic();
    } else {
      const _p$2 = _p;
      _tmp$2 = _p$2;
    }
    _tmp$2.next = entry;
  }
  self.tail = idx;
  const _tmp = self.entries;
  $bound_check(_tmp, idx);
  _tmp[idx] = entry;
  self.size = self.size + 1 | 0;
}
function _M0MPB3Map20add__entry__to__tailGsbE(self, idx, entry) {
  const _bind = self.tail;
  if (_bind === -1) {
    self.head = entry;
  } else {
    const _tmp = self.entries;
    $bound_check(_tmp, _bind);
    const _p = _tmp[_bind];
    let _tmp$2;
    if (_p === undefined) {
      _tmp$2 = $panic();
    } else {
      const _p$2 = _p;
      _tmp$2 = _p$2;
    }
    _tmp$2.next = entry;
  }
  self.tail = idx;
  const _tmp = self.entries;
  $bound_check(_tmp, idx);
  _tmp[idx] = entry;
  self.size = self.size + 1 | 0;
}
function _M0MPB3Map10set__entryGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, entry, new_idx) {
  const _tmp = self.entries;
  $bound_check(_tmp, new_idx);
  _tmp[new_idx] = entry;
  const _bind = entry.next;
  if (_bind === undefined) {
    self.tail = new_idx;
    return;
  } else {
    const _Some = _bind;
    const _next = _Some;
    _next.prev = new_idx;
    return;
  }
}
function _M0MPB3Map10set__entryGsiE(self, entry, new_idx) {
  const _tmp = self.entries;
  $bound_check(_tmp, new_idx);
  _tmp[new_idx] = entry;
  const _bind = entry.next;
  if (_bind === undefined) {
    self.tail = new_idx;
    return;
  } else {
    const _Some = _bind;
    const _next = _Some;
    _next.prev = new_idx;
    return;
  }
}
function _M0MPB3Map10set__entryGsbE(self, entry, new_idx) {
  const _tmp = self.entries;
  $bound_check(_tmp, new_idx);
  _tmp[new_idx] = entry;
  const _bind = entry.next;
  if (_bind === undefined) {
    self.tail = new_idx;
    return;
  } else {
    const _Some = _bind;
    const _next = _Some;
    _next.prev = new_idx;
    return;
  }
}
function _M0MPB3Map10push__awayGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry) {
  let _tmp = entry.psl + 1 | 0;
  let _tmp$2 = idx + 1 & self.capacity_mask;
  let _tmp$3 = entry;
  while (true) {
    const psl = _tmp;
    const idx$2 = _tmp$2;
    const entry$2 = _tmp$3;
    const _tmp$4 = self.entries;
    $bound_check(_tmp$4, idx$2);
    const _bind = _tmp$4[idx$2];
    if (_bind === undefined) {
      entry$2.psl = psl;
      _M0MPB3Map10set__entryGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, entry$2, idx$2);
      return;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (psl > _curr_entry.psl) {
        entry$2.psl = psl;
        _M0MPB3Map10set__entryGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, entry$2, idx$2);
        _tmp = _curr_entry.psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        _tmp$3 = _curr_entry;
        continue;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map10push__awayGsiE(self, idx, entry) {
  let _tmp = entry.psl + 1 | 0;
  let _tmp$2 = idx + 1 & self.capacity_mask;
  let _tmp$3 = entry;
  while (true) {
    const psl = _tmp;
    const idx$2 = _tmp$2;
    const entry$2 = _tmp$3;
    const _tmp$4 = self.entries;
    $bound_check(_tmp$4, idx$2);
    const _bind = _tmp$4[idx$2];
    if (_bind === undefined) {
      entry$2.psl = psl;
      _M0MPB3Map10set__entryGsiE(self, entry$2, idx$2);
      return;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (psl > _curr_entry.psl) {
        entry$2.psl = psl;
        _M0MPB3Map10set__entryGsiE(self, entry$2, idx$2);
        _tmp = _curr_entry.psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        _tmp$3 = _curr_entry;
        continue;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map10push__awayGsbE(self, idx, entry) {
  let _tmp = entry.psl + 1 | 0;
  let _tmp$2 = idx + 1 & self.capacity_mask;
  let _tmp$3 = entry;
  while (true) {
    const psl = _tmp;
    const idx$2 = _tmp$2;
    const entry$2 = _tmp$3;
    const _tmp$4 = self.entries;
    $bound_check(_tmp$4, idx$2);
    const _bind = _tmp$4[idx$2];
    if (_bind === undefined) {
      entry$2.psl = psl;
      _M0MPB3Map10set__entryGsbE(self, entry$2, idx$2);
      return;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (psl > _curr_entry.psl) {
        entry$2.psl = psl;
        _M0MPB3Map10set__entryGsbE(self, entry$2, idx$2);
        _tmp = _curr_entry.psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        _tmp$3 = _curr_entry;
        continue;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx$2 + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map20rehash__place__entryGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, outer) {
  const hash = outer.hash;
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      outer.psl = psl;
      outer.prev = self.tail;
      _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, outer);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr = _Some;
      if (psl > _curr.psl) {
        _M0MPB3Map10push__awayGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, _curr);
        outer.psl = psl;
        outer.prev = self.tail;
        _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, outer);
        return undefined;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map20rehash__place__entryGsiE(self, outer) {
  const hash = outer.hash;
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      outer.psl = psl;
      outer.prev = self.tail;
      _M0MPB3Map20add__entry__to__tailGsiE(self, idx, outer);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr = _Some;
      if (psl > _curr.psl) {
        _M0MPB3Map10push__awayGsiE(self, idx, _curr);
        outer.psl = psl;
        outer.prev = self.tail;
        _M0MPB3Map20add__entry__to__tailGsiE(self, idx, outer);
        return undefined;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map20rehash__place__entryGsbE(self, outer) {
  const hash = outer.hash;
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      outer.psl = psl;
      outer.prev = self.tail;
      _M0MPB3Map20add__entry__to__tailGsbE(self, idx, outer);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr = _Some;
      if (psl > _curr.psl) {
        _M0MPB3Map10push__awayGsbE(self, idx, _curr);
        outer.psl = psl;
        outer.prev = self.tail;
        _M0MPB3Map20add__entry__to__tailGsbE(self, idx, outer);
        return undefined;
      } else {
        _tmp = psl + 1 | 0;
        _tmp$2 = idx + 1 & self.capacity_mask;
        continue;
      }
    }
  }
}
function _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self) {
  const old_head = self.head;
  const new_capacity = self.capacity << 1;
  self.entries = $make_array_len_and_init(new_capacity, undefined);
  self.capacity = new_capacity;
  self.capacity_mask = new_capacity - 1 | 0;
  const _p = self.capacity;
  self.grow_at = (Math.imul(_p, 13) | 0) / 16 | 0;
  self.size = 0;
  self.head = undefined;
  self.tail = -1;
  let _tmp = old_head;
  while (true) {
    const x = _tmp;
    if (x === undefined) {
      return;
    } else {
      const _Some = x;
      const _e = _Some;
      const next_in_chain = _e.next;
      _e.next = undefined;
      _M0MPB3Map20rehash__place__entryGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, _e);
      _tmp = next_in_chain;
      continue;
    }
  }
}
function _M0MPB3Map4growGsiE(self) {
  const old_head = self.head;
  const new_capacity = self.capacity << 1;
  self.entries = $make_array_len_and_init(new_capacity, undefined);
  self.capacity = new_capacity;
  self.capacity_mask = new_capacity - 1 | 0;
  const _p = self.capacity;
  self.grow_at = (Math.imul(_p, 13) | 0) / 16 | 0;
  self.size = 0;
  self.head = undefined;
  self.tail = -1;
  let _tmp = old_head;
  while (true) {
    const x = _tmp;
    if (x === undefined) {
      return;
    } else {
      const _Some = x;
      const _e = _Some;
      const next_in_chain = _e.next;
      _e.next = undefined;
      _M0MPB3Map20rehash__place__entryGsiE(self, _e);
      _tmp = next_in_chain;
      continue;
    }
  }
}
function _M0MPB3Map4growGsbE(self) {
  const old_head = self.head;
  const new_capacity = self.capacity << 1;
  self.entries = $make_array_len_and_init(new_capacity, undefined);
  self.capacity = new_capacity;
  self.capacity_mask = new_capacity - 1 | 0;
  const _p = self.capacity;
  self.grow_at = (Math.imul(_p, 13) | 0) / 16 | 0;
  self.size = 0;
  self.head = undefined;
  self.tail = -1;
  let _tmp = old_head;
  while (true) {
    const x = _tmp;
    if (x === undefined) {
      return;
    } else {
      const _Some = x;
      const _e = _Some;
      const next_in_chain = _e.next;
      _e.next = undefined;
      _M0MPB3Map20rehash__place__entryGsbE(self, _e);
      _tmp = next_in_chain;
      continue;
    }
  }
}
function _M0MPB3Map15set__with__hashGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, key, value, hash) {
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      if (self.size >= self.grow_at) {
        _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
        _tmp = 0;
        _tmp$2 = hash & self.capacity_mask;
        continue;
      }
      const _bind$2 = self.tail;
      const _bind$3 = undefined;
      const entry = new _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8RaftNodeE(_bind$2, _bind$3, psl, hash, key, value);
      _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (_curr_entry.hash === hash && _curr_entry.key === key) {
        _curr_entry.value = value;
        return undefined;
      }
      if (psl > _curr_entry.psl) {
        if (self.size >= self.grow_at) {
          _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
          _tmp = 0;
          _tmp$2 = hash & self.capacity_mask;
          continue;
        }
        _M0MPB3Map10push__awayGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, _curr_entry);
        const _bind$2 = self.tail;
        const _bind$3 = undefined;
        const entry = new _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8RaftNodeE(_bind$2, _bind$3, psl, hash, key, value);
        _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
        return undefined;
      }
      _tmp = psl + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map15set__with__hashGsiE(self, key, value, hash) {
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      if (self.size >= self.grow_at) {
        _M0MPB3Map4growGsiE(self);
        _tmp = 0;
        _tmp$2 = hash & self.capacity_mask;
        continue;
      }
      const _bind$2 = self.tail;
      const _bind$3 = undefined;
      const entry = new _M0TPB5EntryGsiE(_bind$2, _bind$3, psl, hash, key, value);
      _M0MPB3Map20add__entry__to__tailGsiE(self, idx, entry);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (_curr_entry.hash === hash && _curr_entry.key === key) {
        _curr_entry.value = value;
        return undefined;
      }
      if (psl > _curr_entry.psl) {
        if (self.size >= self.grow_at) {
          _M0MPB3Map4growGsiE(self);
          _tmp = 0;
          _tmp$2 = hash & self.capacity_mask;
          continue;
        }
        _M0MPB3Map10push__awayGsiE(self, idx, _curr_entry);
        const _bind$2 = self.tail;
        const _bind$3 = undefined;
        const entry = new _M0TPB5EntryGsiE(_bind$2, _bind$3, psl, hash, key, value);
        _M0MPB3Map20add__entry__to__tailGsiE(self, idx, entry);
        return undefined;
      }
      _tmp = psl + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map15set__with__hashGsbE(self, key, value, hash) {
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      if (self.size >= self.grow_at) {
        _M0MPB3Map4growGsbE(self);
        _tmp = 0;
        _tmp$2 = hash & self.capacity_mask;
        continue;
      }
      const _bind$2 = self.tail;
      const _bind$3 = undefined;
      const entry = new _M0TPB5EntryGsbE(_bind$2, _bind$3, psl, hash, key, value);
      _M0MPB3Map20add__entry__to__tailGsbE(self, idx, entry);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (_curr_entry.hash === hash && _curr_entry.key === key) {
        _curr_entry.value = value;
        return undefined;
      }
      if (psl > _curr_entry.psl) {
        if (self.size >= self.grow_at) {
          _M0MPB3Map4growGsbE(self);
          _tmp = 0;
          _tmp$2 = hash & self.capacity_mask;
          continue;
        }
        _M0MPB3Map10push__awayGsbE(self, idx, _curr_entry);
        const _bind$2 = self.tail;
        const _bind$3 = undefined;
        const entry = new _M0TPB5EntryGsbE(_bind$2, _bind$3, psl, hash, key, value);
        _M0MPB3Map20add__entry__to__tailGsbE(self, idx, entry);
        return undefined;
      }
      _tmp = psl + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map15set__with__hashGmsE(self, key, value, hash) {
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      if (self.size >= self.grow_at) {
        _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
        _tmp = 0;
        _tmp$2 = hash & self.capacity_mask;
        continue;
      }
      const _bind$2 = self.tail;
      const _bind$3 = undefined;
      const entry = new _M0TPB5EntryGmsE(_bind$2, _bind$3, psl, hash, key, value);
      _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (_curr_entry.hash === hash && BigInt.asUintN(64, _curr_entry.key) === BigInt.asUintN(64, key)) {
        _curr_entry.value = value;
        return undefined;
      }
      if (psl > _curr_entry.psl) {
        if (self.size >= self.grow_at) {
          _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
          _tmp = 0;
          _tmp$2 = hash & self.capacity_mask;
          continue;
        }
        _M0MPB3Map10push__awayGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, _curr_entry);
        const _bind$2 = self.tail;
        const _bind$3 = undefined;
        const entry = new _M0TPB5EntryGmsE(_bind$2, _bind$3, psl, hash, key, value);
        _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
        return undefined;
      }
      _tmp = psl + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map15set__with__hashGsRP28heke122814raft_2dmoonbit8ProgressE(self, key, value, hash) {
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const psl = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      if (self.size >= self.grow_at) {
        _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
        _tmp = 0;
        _tmp$2 = hash & self.capacity_mask;
        continue;
      }
      const _bind$2 = self.tail;
      const _bind$3 = undefined;
      const entry = new _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8ProgressE(_bind$2, _bind$3, psl, hash, key, value);
      _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
      return undefined;
    } else {
      const _Some = _bind;
      const _curr_entry = _Some;
      if (_curr_entry.hash === hash && _curr_entry.key === key) {
        _curr_entry.value = value;
        return undefined;
      }
      if (psl > _curr_entry.psl) {
        if (self.size >= self.grow_at) {
          _M0MPB3Map4growGsRP28heke122814raft_2dmoonbit8RaftNodeE(self);
          _tmp = 0;
          _tmp$2 = hash & self.capacity_mask;
          continue;
        }
        _M0MPB3Map10push__awayGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, _curr_entry);
        const _bind$2 = self.tail;
        const _bind$3 = undefined;
        const entry = new _M0TPB5EntryGsRP28heke122814raft_2dmoonbit8ProgressE(_bind$2, _bind$3, psl, hash, key, value);
        _M0MPB3Map20add__entry__to__tailGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, idx, entry);
        return undefined;
      }
      _tmp = psl + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, key, value) {
  _M0MPB3Map15set__with__hashGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, key, value, _M0IPC16string6StringPB4Hash4hash(key));
}
function _M0MPB3Map3setGsiE(self, key, value) {
  _M0MPB3Map15set__with__hashGsiE(self, key, value, _M0IPC16string6StringPB4Hash4hash(key));
}
function _M0MPB3Map3setGsbE(self, key, value) {
  _M0MPB3Map15set__with__hashGsbE(self, key, value, _M0IPC16string6StringPB4Hash4hash(key));
}
function _M0MPB3Map3setGmsE(self, key, value) {
  _M0MPB3Map15set__with__hashGmsE(self, key, value, _M0IPC16uint646UInt64PB4Hash4hash(key));
}
function _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8ProgressE(self, key, value) {
  _M0MPB3Map15set__with__hashGsRP28heke122814raft_2dmoonbit8ProgressE(self, key, value, _M0IPC16string6StringPB4Hash4hash(key));
}
function _M0MPB3Map3MapGsRP28heke122814raft_2dmoonbit8RaftNodeE(arr, capacity) {
  const length = arr.end - arr.start | 0;
  let capacity$2;
  if (capacity === undefined) {
    capacity$2 = length === 0 ? 8 : _M0FPB21capacity__for__length(length);
  } else {
    const _Some = capacity;
    const _capacity = _Some;
    const _p = _M0FPB21capacity__for__length(length);
    capacity$2 = _capacity > _p ? _capacity : _p;
  }
  const m = _M0FPB8new__mapGsRP28heke122814raft_2dmoonbit8RaftNodeE(capacity$2);
  const _bind = arr.end - arr.start | 0;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const e = arr.buf[arr.start + _ | 0];
      _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8RaftNodeE(m, e._0, e._1);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return m;
}
function _M0MPB3Map3MapGsiE(arr, capacity) {
  const length = arr.end - arr.start | 0;
  let capacity$2;
  if (capacity === undefined) {
    capacity$2 = length === 0 ? 8 : _M0FPB21capacity__for__length(length);
  } else {
    const _Some = capacity;
    const _capacity = _Some;
    const _p = _M0FPB21capacity__for__length(length);
    capacity$2 = _capacity > _p ? _capacity : _p;
  }
  const m = _M0FPB8new__mapGsiE(capacity$2);
  const _bind = arr.end - arr.start | 0;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const e = arr.buf[arr.start + _ | 0];
      _M0MPB3Map3setGsiE(m, e._0, e._1);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return m;
}
function _M0MPB3Map3MapGsbE(arr, capacity) {
  const length = arr.end - arr.start | 0;
  let capacity$2;
  if (capacity === undefined) {
    capacity$2 = length === 0 ? 8 : _M0FPB21capacity__for__length(length);
  } else {
    const _Some = capacity;
    const _capacity = _Some;
    const _p = _M0FPB21capacity__for__length(length);
    capacity$2 = _capacity > _p ? _capacity : _p;
  }
  const m = _M0FPB8new__mapGsbE(capacity$2);
  const _bind = arr.end - arr.start | 0;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const e = arr.buf[arr.start + _ | 0];
      _M0MPB3Map3setGsbE(m, e._0, e._1);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return m;
}
function _M0MPB3Map3MapGsRP28heke122814raft_2dmoonbit8ProgressE(arr, capacity) {
  const length = arr.end - arr.start | 0;
  let capacity$2;
  if (capacity === undefined) {
    capacity$2 = length === 0 ? 8 : _M0FPB21capacity__for__length(length);
  } else {
    const _Some = capacity;
    const _capacity = _Some;
    const _p = _M0FPB21capacity__for__length(length);
    capacity$2 = _capacity > _p ? _capacity : _p;
  }
  const m = _M0FPB8new__mapGsRP28heke122814raft_2dmoonbit8RaftNodeE(capacity$2);
  const _bind = arr.end - arr.start | 0;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const e = arr.buf[arr.start + _ | 0];
      _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8ProgressE(m, e._0, e._1);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return m;
}
function _M0MPB3Map3MapGmsE(arr, capacity) {
  const length = arr.end - arr.start | 0;
  let capacity$2;
  if (capacity === undefined) {
    capacity$2 = length === 0 ? 8 : _M0FPB21capacity__for__length(length);
  } else {
    const _Some = capacity;
    const _capacity = _Some;
    const _p = _M0FPB21capacity__for__length(length);
    capacity$2 = _capacity > _p ? _capacity : _p;
  }
  const m = _M0FPB8new__mapGsRP28heke122814raft_2dmoonbit8RaftNodeE(capacity$2);
  const _bind = arr.end - arr.start | 0;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const e = arr.buf[arr.start + _ | 0];
      _M0MPB3Map3setGmsE(m, e._0, e._1);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return m;
}
function _M0MPB3Map3getGsbE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      return -1;
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i > _entry.psl) {
        return -1;
      }
      _tmp = i + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      return undefined;
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i > _entry.psl) {
        return undefined;
      }
      _tmp = i + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map3getGsiE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      return undefined;
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i > _entry.psl) {
        return undefined;
      }
      _tmp = i + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8ProgressE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      return undefined;
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i > _entry.psl) {
        return undefined;
      }
      _tmp = i + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map3getGmsE(self, key) {
  const hash = _M0IPC16uint646UInt64PB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      return undefined;
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && BigInt.asUintN(64, _entry.key) === BigInt.asUintN(64, key)) {
        return _entry.value;
      }
      if (i > _entry.psl) {
        return undefined;
      }
      _tmp = i + 1 | 0;
      _tmp$2 = idx + 1 & self.capacity_mask;
      continue;
    }
  }
}
function _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      $panic();
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i <= _entry.psl) {
        _tmp = i + 1 | 0;
        _tmp$2 = idx + 1 & self.capacity_mask;
        continue;
      } else {
        $panic();
      }
    }
    continue;
  }
}
function _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8ProgressE(self, key) {
  const hash = _M0IPC16string6StringPB4Hash4hash(key);
  let _tmp = 0;
  let _tmp$2 = hash & self.capacity_mask;
  while (true) {
    const i = _tmp;
    const idx = _tmp$2;
    const _tmp$3 = self.entries;
    $bound_check(_tmp$3, idx);
    const _bind = _tmp$3[idx];
    if (_bind === undefined) {
      $panic();
    } else {
      const _Some = _bind;
      const _entry = _Some;
      if (_entry.hash === hash && _entry.key === key) {
        return _entry.value;
      }
      if (i <= _entry.psl) {
        _tmp = i + 1 | 0;
        _tmp$2 = idx + 1 & self.capacity_mask;
        continue;
      } else {
        $panic();
      }
    }
    continue;
  }
}
function _M0MPC15array10FixedArray12fill_2einnerGORPB5EntryGsbEE(self, value, start, end) {
  const array_length = self.length;
  if (array_length > 0) {
    if (start >= 0 && start < array_length) {
      let length;
      if (end === undefined) {
        length = array_length - start | 0;
      } else {
        const _Some = end;
        const _e = _Some;
        length = _e >= start && _e <= array_length ? _e - start | 0 : $panic();
      }
      self.fill(value, start, start + length);
      return;
    } else {
      $panic();
      return;
    }
  } else {
    return;
  }
}
function _M0MPB3Map5clearGsbE(self) {
  _M0MPC15array10FixedArray12fill_2einnerGORPB5EntryGsbEE(self.entries, undefined, 0, undefined);
  self.size = 0;
  self.head = undefined;
  self.tail = -1;
}
function _M0MPB3Map5clearGsRP28heke122814raft_2dmoonbit8ProgressE(self) {
  _M0MPC15array10FixedArray12fill_2einnerGORPB5EntryGsbEE(self.entries, undefined, 0, undefined);
  self.size = 0;
  self.head = undefined;
  self.tail = -1;
}
function _M0MPB3Map4iterGsRP28heke122814raft_2dmoonbit8ProgressE(self) {
  const curr_entry = new _M0TPB8MutLocalGORPB5EntryGsRP28heke122814raft_2dmoonbit8ProgressEE(self.head);
  const len = self.size;
  const remaining = new _M0TPB8MutLocalGiE(len);
  return _M0MPB4Iter3newGUsRP28heke122814raft_2dmoonbit8ProgressEE(() => {
    _L: {
      if (remaining.val > 0) {
        const _bind = curr_entry.val;
        if (_bind === undefined) {
          break _L;
        } else {
          const _Some = _bind;
          const _x = _Some;
          const _key = _x.key;
          const _value = _x.value;
          const _next = _x.next;
          curr_entry.val = _next;
          remaining.val = remaining.val - 1 | 0;
          return { _0: _key, _1: _value };
        }
      } else {
        break _L;
      }
    }
    return undefined;
  }, len);
}
function _M0MPB3Map4iterGsbE(self) {
  const curr_entry = new _M0TPB8MutLocalGORPB5EntryGsbEE(self.head);
  const len = self.size;
  const remaining = new _M0TPB8MutLocalGiE(len);
  return _M0MPB4Iter3newGUsRP28heke122814raft_2dmoonbit8ProgressEE(() => {
    _L: {
      if (remaining.val > 0) {
        const _bind = curr_entry.val;
        if (_bind === undefined) {
          break _L;
        } else {
          const _Some = _bind;
          const _x = _Some;
          const _key = _x.key;
          const _value = _x.value;
          const _next = _x.next;
          curr_entry.val = _next;
          remaining.val = remaining.val - 1 | 0;
          return { _0: _key, _1: _value };
        }
      } else {
        break _L;
      }
    }
    return undefined;
  }, len);
}
function _M0MPB3Map5iter2GsRP28heke122814raft_2dmoonbit8ProgressE(self) {
  return _M0MPB3Map4iterGsRP28heke122814raft_2dmoonbit8ProgressE(self);
}
function _M0MPB3Map5iter2GsbE(self) {
  return _M0MPB3Map4iterGsbE(self);
}
function _M0MPB5Iter24nextGsRP28heke122814raft_2dmoonbit8ProgressE(self) {
  return _M0MPB4Iter4nextGUsRP28heke122814raft_2dmoonbit8ProgressEE(self);
}
function _M0MPB5Iter24nextGsbE(self) {
  return _M0MPB4Iter4nextGUsRP28heke122814raft_2dmoonbit8ProgressEE(self);
}
function _M0IPC16string6StringPB4Hash4hash(self) {
  let acc = (_M0FPB4seed >>> 0) + (374761393 >>> 0) | 0;
  const _bind = self.length;
  let _tmp = 0;
  while (true) {
    const i = _tmp;
    if (i < _bind) {
      acc = (acc >>> 0) + (4 >>> 0) | 0;
      const v = self.charCodeAt(i);
      acc = _M0FPB13consume4__acc(acc, v);
      _tmp = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return _M0FPB13finalize__acc(acc);
}
function _M0IPC16uint646UInt64PB4Hash4hash(self) {
  const acc = (((_M0FPB4seed >>> 0) + (374761393 >>> 0) | 0) >>> 0) + (8 >>> 0) | 0;
  const low = Number(BigInt.asUintN(32, BigInt.asUintN(64, self & 4294967295n))) | 0;
  const high = Number(BigInt.asUintN(32, BigInt.asUintN(64, BigInt.asUintN(64, BigInt.asUintN(64, self) >> BigInt(32 & 63)) & 4294967295n))) | 0;
  const acc$2 = _M0FPB13consume4__acc(acc, low);
  const acc$3 = _M0FPB13consume4__acc(acc$2, high);
  return _M0FPB13finalize__acc(acc$3);
}
function _M0MPC15bytes5Bytes5makei(length, value) {
  if (length <= 0) {
    return $bytes_literal$0;
  }
  const arr = $makebytes(length, value(0));
  let _tmp = 1;
  while (true) {
    const i = _tmp;
    if (i < length) {
      $bound_check(arr, i);
      arr[i] = value(i);
      _tmp = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return arr;
}
function _M0MPC15bytes5Bytes11from__array(arr) {
  return _M0MPC15bytes5Bytes5makei(arr.end - arr.start | 0, (i) => {
    if (i < 0 || i >= (arr.end - arr.start | 0)) {
      $panic();
    }
    return arr.buf[arr.start + i | 0];
  });
}
function _M0MPC15array5Array28unsafe__truncate__to__lengthGRP28heke122814raft_2dmoonbit5EntryE(self, new_len) {
  _M0MPB7JSArray11set__length(self, new_len);
}
function _M0MPC15array5Array11unsafe__popGRP28heke122814raft_2dmoonbit5EntryE(self) {
  return _M0MPB7JSArray3pop(self);
}
function _M0MPC15array5Array3popGRP28heke122814raft_2dmoonbit5EntryE(self) {
  if (self.length === 0) {
    return undefined;
  } else {
    const v = _M0MPC15array5Array11unsafe__popGRP28heke122814raft_2dmoonbit5EntryE(self);
    return v;
  }
}
function _M0MPC15array5Array4copyGsE(self) {
  return _M0MPB7JSArray4copy(self);
}
function _M0MPC15array5Array2atGsE(self, index) {
  const len = self.length;
  return index >= 0 && index < len ? self[index] : $panic();
}
function _M0MPC15array5Array5clearGRP28heke122814raft_2dmoonbit5EntryE(self) {
  _M0MPC15array5Array28unsafe__truncate__to__lengthGRP28heke122814raft_2dmoonbit5EntryE(self, 0);
}
function _M0MPC15array5Array8containsGsE(self, value) {
  const _bind = self.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const v = self[_];
      if (v === value) {
        return true;
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      return false;
    }
  }
}
function _M0IP28heke122814raft_2dmoonbit13ProgressStatePB2Eq5equal(_x_673, _x_674) {
  switch (_x_673) {
    case 0: {
      if (_x_674 === 0) {
        return true;
      } else {
        return false;
      }
    }
    case 1: {
      if (_x_674 === 1) {
        return true;
      } else {
        return false;
      }
    }
    default: {
      if (_x_674 === 2) {
        return true;
      } else {
        return false;
      }
    }
  }
}
function _M0IP28heke122814raft_2dmoonbit9EntryTypePB2Eq5equal(_x_665, _x_666) {
  if (_x_665 === 0) {
    if (_x_666 === 0) {
      return true;
    } else {
      return false;
    }
  } else {
    if (_x_666 === 1) {
      return true;
    } else {
      return false;
    }
  }
}
function _M0IP28heke122814raft_2dmoonbit4RolePB2Eq5equal(_x_625, _x_626) {
  switch (_x_625) {
    case 0: {
      if (_x_626 === 0) {
        return true;
      } else {
        return false;
      }
    }
    case 1: {
      if (_x_626 === 1) {
        return true;
      } else {
        return false;
      }
    }
    default: {
      if (_x_626 === 2) {
        return true;
      } else {
        return false;
      }
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, index) {
  if (BigInt.asUintN(64, index) === BigInt.asUintN(64, self.snapshot_index)) {
    return self.snapshot_term;
  } else {
    let _tmp;
    if (BigInt.asUintN(64, index) <= BigInt.asUintN(64, self.snapshot_index)) {
      _tmp = true;
    } else {
      const _p = self.log.length;
      _tmp = BigInt.asUintN(64, index) > BigInt.asUintN(64, _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index);
    }
    if (_tmp) {
      return 0n;
    } else {
      return _M0MPC15array5Array2atGsE(self.log, Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, index - self.snapshot_index) - 1n))) | 0).term;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit4Node14truncate__from(self, index) {
  const keep = Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, index - self.snapshot_index) - 1n))) | 0;
  while (true) {
    if (self.log.length > keep) {
      _M0MPC15array5Array3popGRP28heke122814raft_2dmoonbit5EntryE(self.log);
      continue;
    } else {
      return;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit5Entry6normal(term, index, command) {
  return new _M0TP28heke122814raft_2dmoonbit5Entry(term, index, 0, command);
}
function _M0MP28heke122814raft_2dmoonbit5Entry16is__conf__change(self) {
  return _M0IP28heke122814raft_2dmoonbit9EntryTypePB2Eq5equal(self.entry_type, 1);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode6status(self) {
  const _tmp = self.id;
  const _p = self.core;
  const _tmp$2 = _p.role;
  const _p$2 = self.core;
  const _tmp$3 = _p$2.current_term;
  const _tmp$4 = self.leader_id;
  const _tmp$5 = self.core.commit_index;
  const _p$3 = self.core;
  const _p$4 = _p$3.log.length;
  return new _M0TP28heke122814raft_2dmoonbit10RaftStatus(_tmp, _tmp$2, _tmp$3, _tmp$4, _tmp$5, _p$4 === 0 ? _p$3.snapshot_index : _M0MPC15array5Array2atGsE(_p$3.log, _p$4 - 1 | 0).index, self.core.last_applied);
}
function _M0MP28heke122814raft_2dmoonbit4Node3new(id) {
  return new _M0TP28heke122814raft_2dmoonbit4Node(id, 0, 0n, undefined, [], 0n, 0n, 0n, 0n);
}
function _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self, term) {
  self.role = 0;
  self.current_term = term;
  self.voted_for = undefined;
}
function _M0MP28heke122814raft_2dmoonbit4Node17become__candidate(self) {
  self.current_term = BigInt.asUintN(64, self.current_term + 1n);
  self.role = 1;
  self.voted_for = self.id;
}
function _M0MP28heke122814raft_2dmoonbit4Node14become__leader(self) {
  self.role = 2;
}
function _M0MP28heke122814raft_2dmoonbit4Node15advance__commit(self, index) {
  if (BigInt.asUintN(64, index) > BigInt.asUintN(64, self.commit_index)) {
    self.commit_index = index;
    return;
  } else {
    return;
  }
}
function _M0MP28heke122814raft_2dmoonbit19InstallSnapshotArgs5whole(term, leader_id, snapshot) {
  return new _M0TP28heke122814raft_2dmoonbit19InstallSnapshotArgs(term, leader_id, snapshot.last_index, snapshot.last_term, 0n, snapshot.data, true);
}
function _M0MP28heke122814raft_2dmoonbit4Node9entry__at(self, index) {
  let _tmp;
  if (BigInt.asUintN(64, index) <= BigInt.asUintN(64, self.snapshot_index)) {
    _tmp = true;
  } else {
    const _p = self.log.length;
    _tmp = BigInt.asUintN(64, index) > BigInt.asUintN(64, _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index);
  }
  if (_tmp) {
    return undefined;
  } else {
    return _M0MPC15array5Array2atGsE(self.log, Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, index - self.snapshot_index) - 1n))) | 0);
  }
}
function _M0MP28heke122814raft_2dmoonbit4Node14entries__after(self, index) {
  const out = [];
  const _p = self.log.length;
  const last = _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index;
  let i = index;
  while (true) {
    if (BigInt.asUintN(64, i) < BigInt.asUintN(64, last)) {
      const _bind = _M0MP28heke122814raft_2dmoonbit4Node9entry__at(self, BigInt.asUintN(64, i + 1n));
      if (_bind === undefined) {
      } else {
        const _Some = _bind;
        const _e = _Some;
        _M0MPC15array5Array4pushGsE(out, _e);
      }
      i = BigInt.asUintN(64, i + 1n);
      continue;
    } else {
      break;
    }
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit4Node25handle__install__snapshot(self, args) {
  if (BigInt.asUintN(64, args.term) < BigInt.asUintN(64, self.current_term)) {
    return new _M0TP28heke122814raft_2dmoonbit20InstallSnapshotReply(self.current_term);
  }
  if (BigInt.asUintN(64, args.term) > BigInt.asUintN(64, self.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self, args.term);
  } else {
    self.role = 0;
  }
  if (BigInt.asUintN(64, args.last_index) <= BigInt.asUintN(64, self.snapshot_index)) {
    return new _M0TP28heke122814raft_2dmoonbit20InstallSnapshotReply(self.current_term);
  }
  let _tmp;
  const _tmp$2 = args.last_index;
  const _p = self.log.length;
  if (BigInt.asUintN(64, _tmp$2) <= BigInt.asUintN(64, _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index)) {
    _tmp = BigInt.asUintN(64, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, args.last_index)) === BigInt.asUintN(64, args.last_term);
  } else {
    _tmp = false;
  }
  if (_tmp) {
    const keep = _M0MP28heke122814raft_2dmoonbit4Node14entries__after(self, args.last_index);
    _M0MPC15array5Array5clearGRP28heke122814raft_2dmoonbit5EntryE(self.log);
    const _bind = keep.length;
    let _tmp$3 = 0;
    while (true) {
      const _ = _tmp$3;
      if (_ < _bind) {
        const e = keep[_];
        _M0MPC15array5Array4pushGsE(self.log, e);
        _tmp$3 = _ + 1 | 0;
        continue;
      } else {
        break;
      }
    }
  } else {
    _M0MPC15array5Array5clearGRP28heke122814raft_2dmoonbit5EntryE(self.log);
  }
  self.snapshot_index = args.last_index;
  self.snapshot_term = args.last_term;
  if (BigInt.asUintN(64, self.commit_index) < BigInt.asUintN(64, args.last_index)) {
    self.commit_index = args.last_index;
  }
  if (BigInt.asUintN(64, self.last_applied) < BigInt.asUintN(64, args.last_index)) {
    self.last_applied = args.last_index;
  }
  return new _M0TP28heke122814raft_2dmoonbit20InstallSnapshotReply(self.current_term);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode10is__leader(self) {
  const _p = self.core;
  return _M0IP28heke122814raft_2dmoonbit4RolePB2Eq5equal(_p.role, 2);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode4term(self) {
  const _p = self.core;
  return _p.current_term;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster22one__leader__per__term(self) {
  const _bind = [];
  const seen = _M0MPB3Map3MapGmsE(new _M0TPB9ArrayViewGUmsEE(_bind, 0, 0), undefined);
  const _bind$2 = self.ids;
  const _bind$3 = _bind$2.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$3) {
      const id = _bind$2[_];
      const n = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
      if (_M0MP28heke122814raft_2dmoonbit8RaftNode10is__leader(n)) {
        const t = _M0MP28heke122814raft_2dmoonbit8RaftNode4term(n);
        const _bind$4 = _M0MPB3Map3getGmsE(seen, t);
        if (_bind$4 === undefined) {
          _M0MPB3Map3setGmsE(seen, t, id);
        } else {
          return false;
        }
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster11min__commit(self) {
  let m = 18446744073709551615n;
  let any = false;
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster11min__commitN6constrS1516)) {
        any = true;
        const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
        const c = _p.core.commit_index;
        if (BigInt.asUintN(64, c) < BigInt.asUintN(64, m)) {
          m = c;
        }
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return any ? m : 0n;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agrees(self) {
  const upto = _M0MP28heke122814raft_2dmoonbit7Cluster11min__commit(self);
  let base = 0n;
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agreesN6constrS1517)) {
        const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
        const s = _p.core.snapshot_index;
        if (BigInt.asUintN(64, s) > BigInt.asUintN(64, base)) {
          base = s;
        }
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  let i = BigInt.asUintN(64, base + 1n);
  while (true) {
    if (BigInt.asUintN(64, i) <= BigInt.asUintN(64, upto)) {
      let term = undefined;
      const _bind$3 = self.ids;
      const _bind$4 = _bind$3.length;
      let _tmp$2 = 0;
      while (true) {
        const _ = _tmp$2;
        if (_ < _bind$4) {
          const id = _bind$3[_];
          if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agreesN6constrS1518)) {
            const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
            const t = _M0MP28heke122814raft_2dmoonbit4Node8term__at(_p.core, i);
            const _bind$5 = term;
            if (_bind$5 === undefined) {
              term = t;
            } else {
              const _Some = _bind$5;
              const _tt = _Some;
              if (BigInt.asUintN(64, _tt) !== BigInt.asUintN(64, t)) {
                return false;
              }
            }
          }
          _tmp$2 = _ + 1 | 0;
          continue;
        } else {
          break;
        }
      }
      i = BigInt.asUintN(64, i + 1n);
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster16pair__consistent(self, x, y) {
  const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, x);
  const nx = _p.core;
  const _p$2 = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, y);
  const ny = _p$2.core;
  let last;
  const _p$3 = nx.log.length;
  const _tmp = _p$3 === 0 ? nx.snapshot_index : _M0MPC15array5Array2atGsE(nx.log, _p$3 - 1 | 0).index;
  const _p$4 = ny.log.length;
  if (BigInt.asUintN(64, _tmp) < BigInt.asUintN(64, _p$4 === 0 ? ny.snapshot_index : _M0MPC15array5Array2atGsE(ny.log, _p$4 - 1 | 0).index)) {
    const _p$5 = nx.log.length;
    last = _p$5 === 0 ? nx.snapshot_index : _M0MPC15array5Array2atGsE(nx.log, _p$5 - 1 | 0).index;
  } else {
    const _p$5 = ny.log.length;
    last = _p$5 === 0 ? ny.snapshot_index : _M0MPC15array5Array2atGsE(ny.log, _p$5 - 1 | 0).index;
  }
  const base = BigInt.asUintN(64, nx.snapshot_index) > BigInt.asUintN(64, ny.snapshot_index) ? nx.snapshot_index : ny.snapshot_index;
  let i = BigInt.asUintN(64, base + 1n);
  while (true) {
    if (BigInt.asUintN(64, i) <= BigInt.asUintN(64, last)) {
      const _bind = _M0MP28heke122814raft_2dmoonbit4Node9entry__at(nx, i);
      const _bind$2 = _M0MP28heke122814raft_2dmoonbit4Node9entry__at(ny, i);
      if (_bind === undefined) {
      } else {
        const _Some = _bind;
        const _ex = _Some;
        if (_bind$2 === undefined) {
        } else {
          const _Some$2 = _bind$2;
          const _ey = _Some$2;
          if (BigInt.asUintN(64, _ex.term) !== BigInt.asUintN(64, _ey.term)) {
            return false;
          }
        }
      }
      i = BigInt.asUintN(64, i + 1n);
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster16logs__consistent(self) {
  const live = [];
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster16logs__consistentN6constrS1519)) {
        _M0MPC15array5Array4pushGsE(live, id);
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  if (live.length < 2) {
    return true;
  }
  let a = 0;
  while (true) {
    if (a < live.length) {
      let b = a + 1 | 0;
      while (true) {
        if (b < live.length) {
          if (!_M0MP28heke122814raft_2dmoonbit7Cluster16pair__consistent(self, _M0MPC15array5Array2atGsE(live, a), _M0MPC15array5Array2atGsE(live, b))) {
            return false;
          }
          b = b + 1 | 0;
          continue;
        } else {
          break;
        }
      }
      a = a + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster25same__committed__commands(self) {
  const upto = _M0MP28heke122814raft_2dmoonbit7Cluster11min__commit(self);
  let base = 0n;
  const live = [];
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster25same__committed__commandsN6constrS1520)) {
        _M0MPC15array5Array4pushGsE(live, id);
        const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
        const s = _p.core.snapshot_index;
        if (BigInt.asUintN(64, s) > BigInt.asUintN(64, base)) {
          base = s;
        }
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  let i = BigInt.asUintN(64, base + 1n);
  while (true) {
    if (BigInt.asUintN(64, i) <= BigInt.asUintN(64, upto)) {
      let command = undefined;
      const _bind$3 = live.length;
      let _tmp$2 = 0;
      while (true) {
        const _ = _tmp$2;
        if (_ < _bind$3) {
          const id = live[_];
          const _p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
          const _bind$4 = _M0MP28heke122814raft_2dmoonbit4Node9entry__at(_p.core, i);
          if (_bind$4 === undefined) {
            return false;
          } else {
            const _Some = _bind$4;
            const _e = _Some;
            const _bind$5 = command;
            if (_bind$5 === undefined) {
              command = _e.command;
            } else {
              const _Some$2 = _bind$5;
              const _c = _Some$2;
              const _p$2 = _e.command;
              if (!$bytes_equal(_c, _p$2)) {
                return false;
              }
            }
          }
          _tmp$2 = _ + 1 | 0;
          continue;
        } else {
          break;
        }
      }
      i = BigInt.asUintN(64, i + 1n);
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster16invariants__hold(self) {
  return _M0MP28heke122814raft_2dmoonbit7Cluster22one__leader__per__term(self) && (_M0MP28heke122814raft_2dmoonbit7Cluster17committed__agrees(self) && _M0MP28heke122814raft_2dmoonbit7Cluster16logs__consistent(self));
}
function _M0MP28heke122814raft_2dmoonbit10Membership3new(members) {
  return new _M0TP28heke122814raft_2dmoonbit10Membership(_M0MPC15array5Array4copyGsE(members), [], false);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode10next__rand(self) {
  self.rng = BigInt.asUintN(64, BigInt.asUintN(64, self.rng * 6364136223846793005n) + 1442695040888963407n);
  return self.rng;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(self) {
  self.election_elapsed = 0;
  self.heartbeat_elapsed = 0;
  const _tmp = _M0MP28heke122814raft_2dmoonbit8RaftNode10next__rand(self);
  const _p = self.election_timeout;
  const _tmp$2 = BigInt.asUintN(64, BigInt(_p));
  if (_tmp$2 === 0n) {
    $panic();
  }
  const spread = Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, _tmp) % BigInt.asUintN(64, _tmp$2)))) | 0;
  self.randomized_election_timeout = self.election_timeout + spread | 0;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode11new_2einner(id, peers, seed, election_timeout, heartbeat_timeout, max_entries) {
  const all = [id];
  const _bind = peers.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const p = peers[_];
      _M0MPC15array5Array4pushGsE(all, p);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  const _tmp$2 = _M0MP28heke122814raft_2dmoonbit4Node3new(id);
  const _tmp$3 = _M0MPC15array5Array4copyGsE(peers);
  const _tmp$4 = _M0MP28heke122814raft_2dmoonbit10Membership3new(all);
  const _bind$2 = [];
  const _tmp$5 = _M0MPB3Map3MapGsRP28heke122814raft_2dmoonbit8ProgressE(new _M0TPB9ArrayViewGUsRP28heke122814raft_2dmoonbit8ProgressEE(_bind$2, 0, 0), undefined);
  const _bind$3 = [];
  const node = new _M0TP28heke122814raft_2dmoonbit8RaftNode(_tmp$2, id, _tmp$3, _tmp$4, _tmp$5, _M0MPB3Map3MapGsbE(new _M0TPB9ArrayViewGUsbEE(_bind$3, 0, 0), undefined), true, false, false, undefined, 0, 0, election_timeout, heartbeat_timeout, election_timeout, max_entries, seed);
  _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(node);
  return node;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster11new_2einner(ids, seed) {
  const _bind = [];
  const nodes = _M0MPB3Map3MapGsRP28heke122814raft_2dmoonbit8RaftNodeE(new _M0TPB9ArrayViewGUsRP28heke122814raft_2dmoonbit8RaftNodeEE(_bind, 0, 0), undefined);
  const _bind$2 = [];
  const part = _M0MPB3Map3MapGsiE(new _M0TPB9ArrayViewGUsiEE(_bind$2, 0, 0), undefined);
  const _bind$3 = [];
  const down = _M0MPB3Map3MapGsbE(new _M0TPB9ArrayViewGUsbEE(_bind$3, 0, 0), undefined);
  let s = seed;
  const _bind$4 = ids.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$4) {
      const id = ids[_];
      const peers = [];
      const _bind$5 = ids.length;
      let _tmp$2 = 0;
      while (true) {
        const _$2 = _tmp$2;
        if (_$2 < _bind$5) {
          const other = ids[_$2];
          if (!(other === id)) {
            _M0MPC15array5Array4pushGsE(peers, other);
          }
          _tmp$2 = _$2 + 1 | 0;
          continue;
        } else {
          break;
        }
      }
      s = BigInt.asUintN(64, BigInt.asUintN(64, s * 2862933555777941757n) + 3037000493n);
      _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8RaftNodeE(nodes, id, _M0MP28heke122814raft_2dmoonbit8RaftNode11new_2einner(id, peers, s, 10, 1, 256n));
      _M0MPB3Map3setGsiE(part, id, 0);
      _M0MPB3Map3setGsbE(down, id, false);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return new _M0TP28heke122814raft_2dmoonbit7Cluster(nodes, _M0MPC15array5Array4copyGsE(ids), [], 0, part, down, 0, 0, 100, seed);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster4rand(self) {
  self.rng = BigInt.asUintN(64, BigInt.asUintN(64, self.rng * 6364136223846793005n) + 1442695040888963407n);
  return self.rng;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster4node(self, id) {
  return _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster9set__drop(self, permil) {
  self.drop_permil = permil;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster10set__delay(self, max_delay) {
  self.max_delay = max_delay;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster9partition(self, group_a, group_b) {
  const ga = self.next_group;
  const gb = self.next_group + 1 | 0;
  self.next_group = self.next_group + 2 | 0;
  const _bind = group_a.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const id = group_a[_];
      _M0MPB3Map3setGsiE(self.part, id, ga);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  const _bind$2 = group_b.length;
  let _tmp$2 = 0;
  while (true) {
    const _ = _tmp$2;
    if (_ < _bind$2) {
      const id = group_b[_];
      _M0MPB3Map3setGsiE(self.part, id, gb);
      _tmp$2 = _ + 1 | 0;
      continue;
    } else {
      return;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster7isolate(self, id) {
  _M0MPB3Map3setGsiE(self.part, id, self.next_group);
  self.next_group = self.next_group + 1 | 0;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster4heal(self) {
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      _M0MPB3Map3setGsiE(self.part, id, 0);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      return;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster5crash(self, id) {
  _M0MPB3Map3setGsbE(self.down, id, true);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster7restart(self, id) {
  _M0MPB3Map3setGsbE(self.down, id, false);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster8is__down(self, id) {
  return _M0IPC16option6OptionPB2Eq5equalGbE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster8is__downN6constrS1521);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster9reachable(self, from, to) {
  if (_M0IPC16option6OptionPB2Eq5equalGbE(_M0MPB3Map3getGsbE(self.down, from), _M0MP28heke122814raft_2dmoonbit7Cluster9reachableN6constrS1522) || _M0IPC16option6OptionPB2Eq5equalGbE(_M0MPB3Map3getGsbE(self.down, to), _M0MP28heke122814raft_2dmoonbit7Cluster9reachableN6constrS1523)) {
    return false;
  }
  const _bind = _M0MPB3Map3getGsiE(self.part, from);
  let ga;
  if (_bind === undefined) {
    ga = 0;
  } else {
    const _Some = _bind;
    ga = _Some;
  }
  const _bind$2 = _M0MPB3Map3getGsiE(self.part, to);
  let gb;
  if (_bind$2 === undefined) {
    gb = 0;
  } else {
    const _Some = _bind$2;
    gb = _Some;
  }
  return ga === gb;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, msg) {
  let delay;
  if (self.max_delay > 0) {
    const _tmp = _M0MP28heke122814raft_2dmoonbit7Cluster4rand(self);
    const _p = self.max_delay + 1 | 0;
    const _tmp$2 = BigInt.asUintN(64, BigInt(_p));
    if (_tmp$2 === 0n) {
      $panic();
    }
    delay = Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, _tmp) % BigInt.asUintN(64, _tmp$2)))) | 0;
  } else {
    delay = 0;
  }
  _M0MPC15array5Array4pushGsE(self.inflight, new _M0TP28heke122814raft_2dmoonbit8InFlight((self.now + 1 | 0) + delay | 0, msg));
}
function _M0MP28heke122814raft_2dmoonbit7Message4term(self) {
  const _bind = self.payload;
  switch (_bind.$tag) {
    case 0: {
      const _PreVote = _bind;
      const _a = _PreVote._0;
      return _a.term;
    }
    case 1: {
      const _PreVoteResp = _bind;
      const _r = _PreVoteResp._0;
      return _r.term;
    }
    case 2: {
      const _Vote = _bind;
      const _a$2 = _Vote._0;
      return _a$2.term;
    }
    case 3: {
      const _VoteResp = _bind;
      const _r$2 = _VoteResp._0;
      return _r$2.term;
    }
    case 4: {
      const _Append = _bind;
      const _a$3 = _Append._0;
      return _a$3.term;
    }
    case 5: {
      const _AppendResp = _bind;
      const _r$3 = _AppendResp._0;
      return _r$3.term;
    }
    case 6: {
      const _Snapshot = _bind;
      const _a$4 = _Snapshot._0;
      return _a$4.term;
    }
    case 7: {
      const _SnapshotResp = _bind;
      const _r$4 = _SnapshotResp._0;
      return _r$4.term;
    }
    default: {
      const _TimeoutNow = _bind;
      return _TimeoutNow._0;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Message3new(from, to, payload) {
  return new _M0TP28heke122814raft_2dmoonbit7Message(from, to, payload);
}
function _M0MP28heke122814raft_2dmoonbit4Node22first__index__of__term(self, from, term) {
  let i = from;
  while (true) {
    if (BigInt.asUintN(64, i) > BigInt.asUintN(64, BigInt.asUintN(64, self.snapshot_index + 1n)) && BigInt.asUintN(64, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, BigInt.asUintN(64, i - 1n))) === BigInt.asUintN(64, term)) {
      i = BigInt.asUintN(64, i - 1n);
      continue;
    } else {
      break;
    }
  }
  return i;
}
function _M0MP28heke122814raft_2dmoonbit4Node14store__entries(self, prev_index, entries) {
  let idx = prev_index;
  let i = 0;
  while (true) {
    if (i < entries.length) {
      idx = BigInt.asUintN(64, idx + 1n);
      const entry = _M0MPC15array5Array2atGsE(entries, i);
      const _tmp = idx;
      const _p = self.log.length;
      if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index)) {
        _M0MPC15array5Array4pushGsE(self.log, entry);
      } else {
        if (BigInt.asUintN(64, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, idx)) !== BigInt.asUintN(64, entry.term)) {
          _M0MP28heke122814raft_2dmoonbit4Node14truncate__from(self, idx);
          _M0MPC15array5Array4pushGsE(self.log, entry);
        }
      }
      i = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return idx;
}
function _M0MP28heke122814raft_2dmoonbit4Node23handle__append__entries(self, args) {
  if (BigInt.asUintN(64, args.term) < BigInt.asUintN(64, self.current_term)) {
    return new _M0TP28heke122814raft_2dmoonbit18AppendEntriesReply(self.current_term, false, 0n, 0n);
  }
  if (BigInt.asUintN(64, args.term) > BigInt.asUintN(64, self.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self, args.term);
  } else {
    self.role = 0;
  }
  const _tmp = args.prev_log_index;
  const _p = self.log.length;
  if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index)) {
    const _tmp$2 = self.current_term;
    const _p$2 = self.log.length;
    return new _M0TP28heke122814raft_2dmoonbit18AppendEntriesReply(_tmp$2, false, 0n, BigInt.asUintN(64, (_p$2 === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p$2 - 1 | 0).index) + 1n));
  }
  if (BigInt.asUintN(64, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, args.prev_log_index)) !== BigInt.asUintN(64, args.prev_log_term)) {
    const bad_term = _M0MP28heke122814raft_2dmoonbit4Node8term__at(self, args.prev_log_index);
    return new _M0TP28heke122814raft_2dmoonbit18AppendEntriesReply(self.current_term, false, 0n, _M0MP28heke122814raft_2dmoonbit4Node22first__index__of__term(self, args.prev_log_index, bad_term));
  }
  const last_new = _M0MP28heke122814raft_2dmoonbit4Node14store__entries(self, args.prev_log_index, args.entries);
  if (BigInt.asUintN(64, args.leader_commit) > BigInt.asUintN(64, self.commit_index)) {
    self.commit_index = BigInt.asUintN(64, args.leader_commit) < BigInt.asUintN(64, last_new) ? args.leader_commit : last_new;
  }
  return new _M0TP28heke122814raft_2dmoonbit18AppendEntriesReply(self.current_term, true, last_new, 0n);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode14handle__append(self, from, args) {
  const reply = _M0MP28heke122814raft_2dmoonbit4Node23handle__append__entries(self.core, args);
  if (BigInt.asUintN(64, reply.term) === BigInt.asUintN(64, args.term)) {
    self.leader_id = args.leader_id;
    self.election_elapsed = 0;
    self.in_pre_campaign = false;
  }
  return [_M0MP28heke122814raft_2dmoonbit7Message3new(self.id, from, new _M0DTP28heke122814raft_2dmoonbit7Payload10AppendResp(reply))];
}
function _M0MP28heke122814raft_2dmoonbit8Progress13become__probe(self) {
  self.state = 0;
}
function _M0MP28heke122814raft_2dmoonbit8Progress17become__replicate(self) {
  self.state = 1;
  self.next_index = BigInt.asUintN(64, self.match_index + 1n);
}
function _M0MP28heke122814raft_2dmoonbit8Progress12mark__active(self) {
  self.recent_active = true;
}
function _M0MP28heke122814raft_2dmoonbit8Progress15maybe__decrease(self, hint) {
  const floor = BigInt.asUintN(64, self.match_index + 1n);
  const target = BigInt.asUintN(64, hint) > BigInt.asUintN(64, floor) ? hint : floor;
  if (BigInt.asUintN(64, target) < BigInt.asUintN(64, self.next_index)) {
    self.next_index = target;
    return true;
  } else {
    return false;
  }
}
function _M0MP28heke122814raft_2dmoonbit8Progress13maybe__update(self, index) {
  const advanced = BigInt.asUintN(64, index) > BigInt.asUintN(64, self.match_index);
  if (advanced) {
    self.match_index = index;
  }
  if (BigInt.asUintN(64, self.next_index) < BigInt.asUintN(64, BigInt.asUintN(64, index + 1n))) {
    self.next_index = BigInt.asUintN(64, index + 1n);
  }
  return advanced;
}
function _M0MP28heke122814raft_2dmoonbit4Node23entries__after__limited(self, index, max) {
  const out = [];
  const _p = self.log.length;
  const last = _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index;
  let i = index;
  while (true) {
    let _tmp;
    if (BigInt.asUintN(64, i) < BigInt.asUintN(64, last)) {
      const _p$2 = out.length;
      _tmp = BigInt.asUintN(64, BigInt.asUintN(64, BigInt(_p$2))) < BigInt.asUintN(64, max);
    } else {
      _tmp = false;
    }
    if (_tmp) {
      const _bind = _M0MP28heke122814raft_2dmoonbit4Node9entry__at(self, BigInt.asUintN(64, i + 1n));
      if (_bind === undefined) {
      } else {
        const _Some = _bind;
        const _e = _Some;
        _M0MPC15array5Array4pushGsE(out, _e);
      }
      i = BigInt.asUintN(64, i + 1n);
      continue;
    } else {
      break;
    }
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit8Progress16become__snapshot(self, snapshot_index) {
  self.state = 2;
  self.next_index = BigInt.asUintN(64, snapshot_index + 1n);
}
function _M0MP28heke122814raft_2dmoonbit8Progress19optimistic__advance(self, last) {
  if (BigInt.asUintN(64, self.next_index) < BigInt.asUintN(64, BigInt.asUintN(64, last + 1n))) {
    self.next_index = BigInt.asUintN(64, last + 1n);
    return;
  } else {
    return;
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, peer) {
  const p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, peer);
  const prev = BigInt.asUintN(64, p.next_index - 1n);
  if (BigInt.asUintN(64, prev) < BigInt.asUintN(64, self.core.snapshot_index)) {
    _M0MP28heke122814raft_2dmoonbit8Progress16become__snapshot(p, self.core.snapshot_index);
    const snap = new _M0TP28heke122814raft_2dmoonbit8Snapshot(self.core.snapshot_index, self.core.snapshot_term, $bytes_literal$0);
    const _tmp = self.id;
    const _p = self.core;
    return _M0MP28heke122814raft_2dmoonbit7Message3new(_tmp, peer, new _M0DTP28heke122814raft_2dmoonbit7Payload8Snapshot(_M0MP28heke122814raft_2dmoonbit19InstallSnapshotArgs5whole(_p.current_term, self.id, snap)));
  }
  const entries = _M0MP28heke122814raft_2dmoonbit4Node23entries__after__limited(self.core, prev, self.max_entries);
  if (_M0IP28heke122814raft_2dmoonbit13ProgressStatePB2Eq5equal(p.state, 1) && entries.length > 0) {
    _M0MP28heke122814raft_2dmoonbit8Progress19optimistic__advance(p, _M0MPC15array5Array2atGsE(entries, entries.length - 1 | 0).index);
  }
  const _tmp = self.id;
  const _p = self.core;
  return _M0MP28heke122814raft_2dmoonbit7Message3new(_tmp, peer, new _M0DTP28heke122814raft_2dmoonbit7Payload6Append(new _M0TP28heke122814raft_2dmoonbit17AppendEntriesArgs(_p.current_term, self.id, prev, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self.core, prev), entries, self.core.commit_index)));
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode13bcast__append(self) {
  const out = [];
  const _bind = self.peers;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const p = _bind[_];
      _M0MPC15array5Array4pushGsE(out, _M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, p));
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return out;
}
function _M0FP28heke122814raft_2dmoonbit9count__in(voters, granted) {
  let n = 0;
  const _bind = voters.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind) {
      const id = voters[_];
      if (_M0MPC15array5Array8containsGsE(granted, id)) {
        n = n + 1 | 0;
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return n;
}
function _M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self, granted) {
  const inc = _M0FP28heke122814raft_2dmoonbit9count__in(self.members, granted) > (self.members.length / 2 | 0);
  if (!self.joint) {
    return inc;
  }
  const out = _M0FP28heke122814raft_2dmoonbit9count__in(self.outgoing, granted) > (self.outgoing.length / 2 | 0);
  return inc && out;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode13maybe__commit(self) {
  const _p = self.core;
  const _p$2 = _p.log.length;
  const last = _p$2 === 0 ? _p.snapshot_index : _M0MPC15array5Array2atGsE(_p.log, _p$2 - 1 | 0).index;
  let candidate = self.core.commit_index;
  let n = last;
  while (true) {
    if (BigInt.asUintN(64, n) > BigInt.asUintN(64, self.core.commit_index)) {
      const _tmp = _M0MP28heke122814raft_2dmoonbit4Node8term__at(self.core, n);
      const _p$3 = self.core;
      if (BigInt.asUintN(64, _tmp) === BigInt.asUintN(64, _p$3.current_term)) {
        const acked = [self.id];
        const _it = _M0MPB3Map5iter2GsRP28heke122814raft_2dmoonbit8ProgressE(self.progress);
        while (true) {
          const _bind = _M0MPB5Iter24nextGsRP28heke122814raft_2dmoonbit8ProgressE(_it);
          if (_bind === undefined) {
            break;
          } else {
            const _Some = _bind;
            const _x = _Some;
            const _peer = _x._0;
            const _p$4 = _x._1;
            if (BigInt.asUintN(64, _p$4.match_index) >= BigInt.asUintN(64, n)) {
              _M0MPC15array5Array4pushGsE(acked, _peer);
            }
            continue;
          }
        }
        if (_M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, acked)) {
          candidate = n;
          break;
        }
      }
      n = BigInt.asUintN(64, n - 1n);
      continue;
    } else {
      break;
    }
  }
  if (BigInt.asUintN(64, candidate) > BigInt.asUintN(64, self.core.commit_index)) {
    _M0MP28heke122814raft_2dmoonbit4Node15advance__commit(self.core, candidate);
    return true;
  } else {
    return false;
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode20handle__append__resp(self, from, reply) {
  const _p = self.core;
  if (_M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(_p.role, 2)) {
    return [];
  }
  const _tmp = reply.term;
  const _p$2 = self.core;
  if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p$2.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self.core, reply.term);
    return [];
  }
  const _bind = _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, from);
  if (_bind === undefined) {
    return [];
  } else {
    const _Some = _bind;
    const _p$3 = _Some;
    _M0MP28heke122814raft_2dmoonbit8Progress12mark__active(_p$3);
    if (reply.success) {
      _M0MP28heke122814raft_2dmoonbit8Progress13maybe__update(_p$3, reply.match_index);
      _M0MP28heke122814raft_2dmoonbit8Progress17become__replicate(_p$3);
      const out = [];
      if (_M0MP28heke122814raft_2dmoonbit8RaftNode13maybe__commit(self)) {
        const _bind$2 = _M0MP28heke122814raft_2dmoonbit8RaftNode13bcast__append(self);
        const _bind$3 = _bind$2.length;
        let _tmp$2 = 0;
        while (true) {
          const _ = _tmp$2;
          if (_ < _bind$3) {
            const m = _bind$2[_];
            _M0MPC15array5Array4pushGsE(out, m);
            _tmp$2 = _ + 1 | 0;
            continue;
          } else {
            break;
          }
        }
      } else {
        const _tmp$2 = _p$3.match_index;
        const _p$4 = self.core;
        const _p$5 = _p$4.log.length;
        if (BigInt.asUintN(64, _tmp$2) < BigInt.asUintN(64, _p$5 === 0 ? _p$4.snapshot_index : _M0MPC15array5Array2atGsE(_p$4.log, _p$5 - 1 | 0).index)) {
          _M0MPC15array5Array4pushGsE(out, _M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, from));
        }
      }
      return out;
    } else {
      _M0MP28heke122814raft_2dmoonbit8Progress15maybe__decrease(_p$3, reply.conflict_index);
      _M0MP28heke122814raft_2dmoonbit8Progress13become__probe(_p$3);
      return [_M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, from)];
    }
  }
}
function _M0FP28heke122814raft_2dmoonbit17log__up__to__date(core, cand_term, cand_index) {
  const _p = core.log.length;
  const my_term = _p === 0 ? core.snapshot_term : _M0MPC15array5Array2atGsE(core.log, _p - 1 | 0).term;
  if (BigInt.asUintN(64, cand_term) > BigInt.asUintN(64, my_term)) {
    return true;
  } else {
    let _tmp;
    if (BigInt.asUintN(64, cand_term) === BigInt.asUintN(64, my_term)) {
      const _p$2 = core.log.length;
      _tmp = BigInt.asUintN(64, cand_index) >= BigInt.asUintN(64, _p$2 === 0 ? core.snapshot_index : _M0MPC15array5Array2atGsE(core.log, _p$2 - 1 | 0).index);
    } else {
      _tmp = false;
    }
    return _tmp;
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode17handle__pre__vote(self, from, args) {
  let granted;
  const _tmp = args.term;
  const _p = self.core;
  if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p.current_term)) {
    granted = _M0FP28heke122814raft_2dmoonbit17log__up__to__date(self.core, args.last_log_term, args.last_log_index);
  } else {
    granted = false;
  }
  let term;
  if (granted) {
    term = args.term;
  } else {
    const _p$2 = self.core;
    term = _p$2.current_term;
  }
  return [_M0MP28heke122814raft_2dmoonbit7Message3new(self.id, from, new _M0DTP28heke122814raft_2dmoonbit7Payload11PreVoteResp(new _M0TP28heke122814raft_2dmoonbit16RequestVoteReply(term, granted)))];
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode7granted(self) {
  const out = [];
  const _it = _M0MPB3Map5iter2GsbE(self.votes);
  while (true) {
    const _bind = _M0MPB5Iter24nextGsbE(_it);
    if (_bind === undefined) {
      break;
    } else {
      const _Some = _bind;
      const _x = _Some;
      const _id = _x._0;
      const _ok = _x._1;
      if (_ok) {
        _M0MPC15array5Array4pushGsE(out, _id);
      }
      continue;
    }
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit4Node19request__vote__args(self) {
  const _tmp = self.current_term;
  const _tmp$2 = self.id;
  const _p = self.log.length;
  const _tmp$3 = _p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index;
  const _p$2 = self.log.length;
  return new _M0TP28heke122814raft_2dmoonbit15RequestVoteArgs(_tmp, _tmp$2, _tmp$3, _p$2 === 0 ? self.snapshot_term : _M0MPC15array5Array2atGsE(self.log, _p$2 - 1 | 0).term);
}
function _M0MP28heke122814raft_2dmoonbit4Node6append(self, term, command) {
  const _p = self.log.length;
  const entry = _M0MP28heke122814raft_2dmoonbit5Entry6normal(term, BigInt.asUintN(64, (_p === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).index) + 1n), command);
  _M0MPC15array5Array4pushGsE(self.log, entry);
  return entry;
}
function _M0MP28heke122814raft_2dmoonbit8Progress3new(next) {
  return new _M0TP28heke122814raft_2dmoonbit8Progress(next, 0n, 0, false);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode14become__leader(self) {
  _M0MP28heke122814raft_2dmoonbit4Node14become__leader(self.core);
  self.leader_id = self.id;
  _M0MPB3Map5clearGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress);
  const _p = self.core;
  const _p$2 = _p.log.length;
  const last = _p$2 === 0 ? _p.snapshot_index : _M0MPC15array5Array2atGsE(_p.log, _p$2 - 1 | 0).index;
  const _bind = self.peers;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const p = _bind[_];
      _M0MPB3Map3setGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, p, _M0MP28heke122814raft_2dmoonbit8Progress3new(BigInt.asUintN(64, last + 1n)));
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  const _tmp$2 = self.core;
  const _p$3 = self.core;
  _M0MP28heke122814raft_2dmoonbit4Node6append(_tmp$2, _p$3.current_term, $bytes_literal$0);
  self.heartbeat_elapsed = 0;
  self.election_elapsed = 0;
  _M0MP28heke122814raft_2dmoonbit8RaftNode13maybe__commit(self);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode21start__real__campaign(self) {
  self.in_pre_campaign = false;
  _M0MP28heke122814raft_2dmoonbit4Node17become__candidate(self.core);
  self.leader_id = undefined;
  _M0MPB3Map5clearGsbE(self.votes);
  _M0MPB3Map3setGsbE(self.votes, self.id, true);
  const out = [];
  const args = _M0MP28heke122814raft_2dmoonbit4Node19request__vote__args(self.core);
  const _bind = self.peers;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const p = _bind[_];
      _M0MPC15array5Array4pushGsE(out, _M0MP28heke122814raft_2dmoonbit7Message3new(self.id, p, new _M0DTP28heke122814raft_2dmoonbit7Payload4Vote(args)));
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  if (_M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, _M0MP28heke122814raft_2dmoonbit8RaftNode7granted(self))) {
    _M0MP28heke122814raft_2dmoonbit8RaftNode14become__leader(self);
    return _M0MP28heke122814raft_2dmoonbit8RaftNode13bcast__append(self);
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode23handle__pre__vote__resp(self, from, reply) {
  if (!self.in_pre_campaign) {
    return [];
  }
  let _tmp;
  if (!reply.vote_granted) {
    const _tmp$2 = reply.term;
    const _p = self.core;
    _tmp = BigInt.asUintN(64, _tmp$2) > BigInt.asUintN(64, _p.current_term);
  } else {
    _tmp = false;
  }
  if (_tmp) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self.core, reply.term);
    self.in_pre_campaign = false;
    return [];
  }
  _M0MPB3Map3setGsbE(self.votes, from, reply.vote_granted);
  if (_M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, _M0MP28heke122814raft_2dmoonbit8RaftNode7granted(self))) {
    return _M0MP28heke122814raft_2dmoonbit8RaftNode21start__real__campaign(self);
  }
  return [];
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode16handle__snapshot(self, from, args) {
  const reply = _M0MP28heke122814raft_2dmoonbit4Node25handle__install__snapshot(self.core, args);
  if (BigInt.asUintN(64, reply.term) === BigInt.asUintN(64, args.term)) {
    self.leader_id = args.leader_id;
    self.election_elapsed = 0;
    self.in_pre_campaign = false;
  }
  return [_M0MP28heke122814raft_2dmoonbit7Message3new(self.id, from, new _M0DTP28heke122814raft_2dmoonbit7Payload12SnapshotResp(reply))];
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode22handle__snapshot__resp(self, from, reply) {
  const _p = self.core;
  if (_M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(_p.role, 2)) {
    return [];
  }
  const _tmp = reply.term;
  const _p$2 = self.core;
  if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p$2.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self.core, reply.term);
    return [];
  }
  const _bind = _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, from);
  if (_bind === undefined) {
    return [];
  } else {
    const _Some = _bind;
    const _p$3 = _Some;
    _M0MP28heke122814raft_2dmoonbit8Progress13maybe__update(_p$3, self.core.snapshot_index);
    _M0MP28heke122814raft_2dmoonbit8Progress13become__probe(_p$3);
    _M0MP28heke122814raft_2dmoonbit8Progress12mark__active(_p$3);
    return [_M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, from)];
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode20handle__timeout__now(self, term) {
  let _tmp;
  const _p = self.core;
  if (BigInt.asUintN(64, term) < BigInt.asUintN(64, _p.current_term)) {
    _tmp = true;
  } else {
    const _p$2 = self.core;
    _tmp = _M0IP28heke122814raft_2dmoonbit4RolePB2Eq5equal(_p$2.role, 2);
  }
  if (_tmp) {
    return [];
  }
  _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(self);
  return _M0MP28heke122814raft_2dmoonbit8RaftNode21start__real__campaign(self);
}
function _M0MP28heke122814raft_2dmoonbit4Node28candidate__log__up__to__date(self, last_log_term, last_log_index) {
  const _p = self.log.length;
  const my_term = _p === 0 ? self.snapshot_term : _M0MPC15array5Array2atGsE(self.log, _p - 1 | 0).term;
  if (BigInt.asUintN(64, last_log_term) > BigInt.asUintN(64, my_term)) {
    return true;
  } else {
    let _tmp;
    if (BigInt.asUintN(64, last_log_term) === BigInt.asUintN(64, my_term)) {
      const _p$2 = self.log.length;
      _tmp = BigInt.asUintN(64, last_log_index) >= BigInt.asUintN(64, _p$2 === 0 ? self.snapshot_index : _M0MPC15array5Array2atGsE(self.log, _p$2 - 1 | 0).index);
    } else {
      _tmp = false;
    }
    return _tmp;
  }
}
function _M0MP28heke122814raft_2dmoonbit4Node21handle__request__vote(self, args) {
  if (BigInt.asUintN(64, args.term) < BigInt.asUintN(64, self.current_term)) {
    return new _M0TP28heke122814raft_2dmoonbit16RequestVoteReply(self.current_term, false);
  }
  if (BigInt.asUintN(64, args.term) > BigInt.asUintN(64, self.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self, args.term);
  }
  const _bind = self.voted_for;
  let can_vote;
  if (_bind === undefined) {
    can_vote = true;
  } else {
    const _Some = _bind;
    const _id = _Some;
    can_vote = _id === args.candidate_id;
  }
  const granted = can_vote && _M0MP28heke122814raft_2dmoonbit4Node28candidate__log__up__to__date(self, args.last_log_term, args.last_log_index);
  if (granted) {
    self.voted_for = args.candidate_id;
  }
  return new _M0TP28heke122814raft_2dmoonbit16RequestVoteReply(self.current_term, granted);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode12handle__vote(self, from, args) {
  const reply = _M0MP28heke122814raft_2dmoonbit4Node21handle__request__vote(self.core, args);
  if (reply.vote_granted) {
    self.election_elapsed = 0;
    self.leader_id = undefined;
  }
  return [_M0MP28heke122814raft_2dmoonbit7Message3new(self.id, from, new _M0DTP28heke122814raft_2dmoonbit7Payload8VoteResp(reply))];
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode18handle__vote__resp(self, from, reply) {
  const _p = self.core;
  if (_M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(_p.role, 1)) {
    return [];
  }
  const _tmp = reply.term;
  const _p$2 = self.core;
  if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p$2.current_term)) {
    _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self.core, reply.term);
    return [];
  }
  _M0MPB3Map3setGsbE(self.votes, from, reply.vote_granted);
  if (_M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, _M0MP28heke122814raft_2dmoonbit8RaftNode7granted(self))) {
    _M0MP28heke122814raft_2dmoonbit8RaftNode14become__leader(self);
    return _M0MP28heke122814raft_2dmoonbit8RaftNode13bcast__append(self);
  }
  return [];
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode4step(self, msg) {
  const _bind = msg.payload;
  switch (_bind.$tag) {
    case 0: {
      break;
    }
    case 1: {
      break;
    }
    default: {
      const _tmp = _M0MP28heke122814raft_2dmoonbit7Message4term(msg);
      const _p = self.core;
      if (BigInt.asUintN(64, _tmp) > BigInt.asUintN(64, _p.current_term)) {
        _M0MP28heke122814raft_2dmoonbit4Node16become__follower(self.core, _M0MP28heke122814raft_2dmoonbit7Message4term(msg));
        self.in_pre_campaign = false;
        const _bind$2 = msg.payload;
        let _tmp$2;
        switch (_bind$2.$tag) {
          case 4: {
            const _Append = _bind$2;
            const _a = _Append._0;
            _tmp$2 = _a.leader_id;
            break;
          }
          case 6: {
            const _Snapshot = _bind$2;
            const _a$2 = _Snapshot._0;
            _tmp$2 = _a$2.leader_id;
            break;
          }
          default: {
            _tmp$2 = undefined;
          }
        }
        self.leader_id = _tmp$2;
        _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(self);
      }
    }
  }
  const _bind$2 = msg.payload;
  switch (_bind$2.$tag) {
    case 0: {
      const _PreVote = _bind$2;
      const _args = _PreVote._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode17handle__pre__vote(self, msg.from, _args);
    }
    case 1: {
      const _PreVoteResp = _bind$2;
      const _reply = _PreVoteResp._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode23handle__pre__vote__resp(self, msg.from, _reply);
    }
    case 2: {
      const _Vote = _bind$2;
      const _args$2 = _Vote._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode12handle__vote(self, msg.from, _args$2);
    }
    case 3: {
      const _VoteResp = _bind$2;
      const _reply$2 = _VoteResp._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode18handle__vote__resp(self, msg.from, _reply$2);
    }
    case 4: {
      const _Append = _bind$2;
      const _args$3 = _Append._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode14handle__append(self, msg.from, _args$3);
    }
    case 5: {
      const _AppendResp = _bind$2;
      const _reply$3 = _AppendResp._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode20handle__append__resp(self, msg.from, _reply$3);
    }
    case 6: {
      const _Snapshot = _bind$2;
      const _args$4 = _Snapshot._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode16handle__snapshot(self, msg.from, _args$4);
    }
    case 7: {
      const _SnapshotResp = _bind$2;
      const _reply$4 = _SnapshotResp._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode22handle__snapshot__resp(self, msg.from, _reply$4);
    }
    default: {
      const _TimeoutNow = _bind$2;
      const _term = _TimeoutNow._0;
      return _M0MP28heke122814raft_2dmoonbit8RaftNode20handle__timeout__now(self, _term);
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster12deliver__due(self) {
  const keep = [];
  const due = [];
  const _bind = self.inflight;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const f = _bind[_];
      if (f.deliver_at <= self.now) {
        _M0MPC15array5Array4pushGsE(due, f);
      } else {
        _M0MPC15array5Array4pushGsE(keep, f);
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  self.inflight = keep;
  const _bind$3 = due.length;
  let _tmp$2 = 0;
  while (true) {
    const _ = _tmp$2;
    if (_ < _bind$3) {
      const f = due[_];
      const m = f.msg;
      let dropped;
      if (!_M0MP28heke122814raft_2dmoonbit7Cluster9reachable(self, m.from, m.to)) {
        dropped = true;
      } else {
        let _tmp$3;
        if (self.drop_permil > 0) {
          if (1000n === 0n) {
            $panic();
          }
          _tmp$3 = (Number(BigInt.asIntN(32, BigInt.asUintN(64, BigInt.asUintN(64, _M0MP28heke122814raft_2dmoonbit7Cluster4rand(self)) % BigInt.asUintN(64, 1000n)))) | 0) < self.drop_permil;
        } else {
          _tmp$3 = false;
        }
        dropped = _tmp$3;
      }
      if (!dropped) {
        const _bind$4 = _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, m.to);
        if (_bind$4 === undefined) {
        } else {
          const _Some = _bind$4;
          const _n = _Some;
          const _bind$5 = _M0MP28heke122814raft_2dmoonbit8RaftNode4step(_n, m);
          const _bind$6 = _bind$5.length;
          let _tmp$3 = 0;
          while (true) {
            const _$2 = _tmp$3;
            if (_$2 < _bind$6) {
              const r = _bind$5[_$2];
              _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, r);
              _tmp$3 = _$2 + 1 | 0;
              continue;
            } else {
              break;
            }
          }
        }
      }
      _tmp$2 = _ + 1 | 0;
      continue;
    } else {
      return;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode16bcast__heartbeat(self) {
  const out = [];
  const _bind = self.peers;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const peer = _bind[_];
      const p = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, peer);
      let prev;
      const _tmp$2 = p.match_index;
      const _p = self.core;
      const _p$2 = _p.log.length;
      if (BigInt.asUintN(64, _tmp$2) < BigInt.asUintN(64, _p$2 === 0 ? _p.snapshot_index : _M0MPC15array5Array2atGsE(_p.log, _p$2 - 1 | 0).index)) {
        prev = p.match_index;
      } else {
        const _p$3 = self.core;
        const _p$4 = _p$3.log.length;
        prev = _p$4 === 0 ? _p$3.snapshot_index : _M0MPC15array5Array2atGsE(_p$3.log, _p$4 - 1 | 0).index;
      }
      const anchor = BigInt.asUintN(64, prev) < BigInt.asUintN(64, self.core.snapshot_index) ? self.core.snapshot_index : prev;
      const _tmp$3 = self.id;
      const _p$3 = self.core;
      _M0MPC15array5Array4pushGsE(out, _M0MP28heke122814raft_2dmoonbit7Message3new(_tmp$3, peer, new _M0DTP28heke122814raft_2dmoonbit7Payload6Append(new _M0TP28heke122814raft_2dmoonbit17AppendEntriesArgs(_p$3.current_term, self.id, anchor, _M0MP28heke122814raft_2dmoonbit4Node8term__at(self.core, anchor), [], self.core.commit_index))));
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode20start__pre__campaign(self) {
  self.in_pre_campaign = true;
  _M0MPB3Map5clearGsbE(self.votes);
  _M0MPB3Map3setGsbE(self.votes, self.id, true);
  const out = [];
  const _p = self.core;
  const _tmp = BigInt.asUintN(64, _p.current_term + 1n);
  const _tmp$2 = self.id;
  const _p$2 = self.core;
  const _p$3 = _p$2.log.length;
  const _tmp$3 = _p$3 === 0 ? _p$2.snapshot_index : _M0MPC15array5Array2atGsE(_p$2.log, _p$3 - 1 | 0).index;
  const _p$4 = self.core;
  const _p$5 = _p$4.log.length;
  const args = new _M0TP28heke122814raft_2dmoonbit15RequestVoteArgs(_tmp, _tmp$2, _tmp$3, _p$5 === 0 ? _p$4.snapshot_term : _M0MPC15array5Array2atGsE(_p$4.log, _p$5 - 1 | 0).term);
  const _bind = self.peers;
  const _bind$2 = _bind.length;
  let _tmp$4 = 0;
  while (true) {
    const _ = _tmp$4;
    if (_ < _bind$2) {
      const p = _bind[_];
      _M0MPC15array5Array4pushGsE(out, _M0MP28heke122814raft_2dmoonbit7Message3new(self.id, p, new _M0DTP28heke122814raft_2dmoonbit7Payload7PreVote(args)));
      _tmp$4 = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  if (_M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, _M0MP28heke122814raft_2dmoonbit8RaftNode7granted(self))) {
    return _M0MP28heke122814raft_2dmoonbit8RaftNode21start__real__campaign(self);
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode8campaign(self) {
  _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(self);
  return self.pre_vote ? _M0MP28heke122814raft_2dmoonbit8RaftNode20start__pre__campaign(self) : _M0MP28heke122814raft_2dmoonbit8RaftNode21start__real__campaign(self);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode14quorum__active(self) {
  const acked = [self.id];
  const _it = _M0MPB3Map5iter2GsRP28heke122814raft_2dmoonbit8ProgressE(self.progress);
  while (true) {
    const _bind = _M0MPB5Iter24nextGsRP28heke122814raft_2dmoonbit8ProgressE(_it);
    if (_bind === undefined) {
      break;
    } else {
      const _Some = _bind;
      const _x = _Some;
      const _peer = _x._0;
      const _p = _x._1;
      if (_p.recent_active) {
        _M0MPC15array5Array4pushGsE(acked, _peer);
      }
      continue;
    }
  }
  return _M0MP28heke122814raft_2dmoonbit10Membership13has__majority(self.config, acked);
}
function _M0MP28heke122814raft_2dmoonbit8Progress13reset__active(self) {
  self.recent_active = false;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode21reset__recent__active(self) {
  const _it = _M0MPB3Map5iter2GsRP28heke122814raft_2dmoonbit8ProgressE(self.progress);
  while (true) {
    const _bind = _M0MPB5Iter24nextGsRP28heke122814raft_2dmoonbit8ProgressE(_it);
    if (_bind === undefined) {
      return;
    } else {
      const _Some = _bind;
      const _x = _Some;
      const _p = _x._1;
      _M0MP28heke122814raft_2dmoonbit8Progress13reset__active(_p);
      continue;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode10step__down(self) {
  const _tmp = self.core;
  const _p = self.core;
  _M0MP28heke122814raft_2dmoonbit4Node16become__follower(_tmp, _p.current_term);
  self.leader_id = undefined;
  _M0MP28heke122814raft_2dmoonbit8RaftNode14reset__timeout(self);
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode4tick(self) {
  const _p = self.core;
  if (_M0IP28heke122814raft_2dmoonbit4RolePB2Eq5equal(_p.role, 2)) {
    self.heartbeat_elapsed = self.heartbeat_elapsed + 1 | 0;
    self.election_elapsed = self.election_elapsed + 1 | 0;
    const out = [];
    if (self.election_elapsed >= self.election_timeout) {
      self.election_elapsed = 0;
      if (self.check_quorum && !_M0MP28heke122814raft_2dmoonbit8RaftNode14quorum__active(self)) {
        _M0MP28heke122814raft_2dmoonbit8RaftNode10step__down(self);
        return [];
      }
      _M0MP28heke122814raft_2dmoonbit8RaftNode21reset__recent__active(self);
    }
    if (self.heartbeat_elapsed >= self.heartbeat_timeout) {
      self.heartbeat_elapsed = 0;
      const _bind = _M0MP28heke122814raft_2dmoonbit8RaftNode16bcast__heartbeat(self);
      const _bind$2 = _bind.length;
      let _tmp = 0;
      while (true) {
        const _ = _tmp;
        if (_ < _bind$2) {
          const m = _bind[_];
          _M0MPC15array5Array4pushGsE(out, m);
          _tmp = _ + 1 | 0;
          continue;
        } else {
          break;
        }
      }
    }
    return out;
  }
  self.election_elapsed = self.election_elapsed + 1 | 0;
  if (self.election_elapsed >= self.randomized_election_timeout) {
    return _M0MP28heke122814raft_2dmoonbit8RaftNode8campaign(self);
  }
  return [];
}
function _M0MP28heke122814raft_2dmoonbit7Cluster4tick(self) {
  self.now = self.now + 1 | 0;
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster4tickN6constrS1524)) {
        const _bind$3 = _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
        if (_bind$3 === undefined) {
        } else {
          const _Some = _bind$3;
          const _n = _Some;
          const _bind$4 = _M0MP28heke122814raft_2dmoonbit8RaftNode4tick(_n);
          const _bind$5 = _bind$4.length;
          let _tmp$2 = 0;
          while (true) {
            const _$2 = _tmp$2;
            if (_$2 < _bind$5) {
              const msg = _bind$4[_$2];
              _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, msg);
              _tmp$2 = _$2 + 1 | 0;
              continue;
            } else {
              break;
            }
          }
        }
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  _M0MP28heke122814raft_2dmoonbit7Cluster12deliver__due(self);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster3run(self, ticks) {
  let i = 0;
  while (true) {
    if (i < ticks) {
      _M0MP28heke122814raft_2dmoonbit7Cluster4tick(self);
      i = i + 1 | 0;
      continue;
    } else {
      return;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster7leaders(self) {
  const out = [];
  const _bind = self.ids;
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const id = _bind[_];
      if (_M0IP016_24default__implPB2Eq10not__equalGObE(_M0MPB3Map3getGsbE(self.down, id), _M0MP28heke122814raft_2dmoonbit7Cluster7leadersN6constrS1525) && _M0MP28heke122814raft_2dmoonbit8RaftNode10is__leader(_M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id))) {
        _M0MPC15array5Array4pushGsE(out, id);
      }
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return out;
}
function _M0MP28heke122814raft_2dmoonbit7Cluster6leader(self) {
  const ls = _M0MP28heke122814raft_2dmoonbit7Cluster7leaders(self);
  return ls.length >= 1 ? _M0MPC15array5Array2atGsE(ls, 0) : undefined;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode7propose(self, command) {
  const _p = self.core;
  if (_M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(_p.role, 2)) {
    return [];
  }
  const _tmp = self.core;
  const _p$2 = self.core;
  _M0MP28heke122814raft_2dmoonbit4Node6append(_tmp, _p$2.current_term, command);
  _M0MP28heke122814raft_2dmoonbit8RaftNode13maybe__commit(self);
  return _M0MP28heke122814raft_2dmoonbit8RaftNode13bcast__append(self);
}
function _M0MP28heke122814raft_2dmoonbit7Cluster7propose(self, command) {
  const _bind = _M0MP28heke122814raft_2dmoonbit7Cluster6leader(self);
  if (_bind === undefined) {
    return false;
  } else {
    const _Some = _bind;
    const _id = _Some;
    const _bind$2 = _M0MP28heke122814raft_2dmoonbit8RaftNode7propose(_M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, _id), command);
    const _bind$3 = _bind$2.length;
    let _tmp = 0;
    while (true) {
      const _ = _tmp;
      if (_ < _bind$3) {
        const msg = _bind$2[_];
        _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, msg);
        _tmp = _ + 1 | 0;
        continue;
      } else {
        break;
      }
    }
    return true;
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster11propose__on(self, id, command) {
  const n = _M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, id);
  if (!_M0MP28heke122814raft_2dmoonbit8RaftNode10is__leader(n)) {
    return false;
  }
  const _bind = _M0MP28heke122814raft_2dmoonbit8RaftNode7propose(n, command);
  const _bind$2 = _bind.length;
  let _tmp = 0;
  while (true) {
    const _ = _tmp;
    if (_ < _bind$2) {
      const msg = _bind[_];
      _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, msg);
      _tmp = _ + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  return true;
}
function _M0MP28heke122814raft_2dmoonbit8RaftNode20transfer__leadership(self, target) {
  const _p = self.core;
  if (_M0IP016_24default__implPB2Eq10not__equalGRP28heke122814raft_2dmoonbit4RoleE(_p.role, 2)) {
    return [];
  }
  const _bind = _M0MPB3Map3getGsRP28heke122814raft_2dmoonbit8ProgressE(self.progress, target);
  if (_bind === undefined) {
    return [];
  } else {
    const _Some = _bind;
    const _p$2 = _Some;
    const _tmp = _p$2.match_index;
    const _p$3 = self.core;
    const _p$4 = _p$3.log.length;
    if (BigInt.asUintN(64, _tmp) === BigInt.asUintN(64, _p$4 === 0 ? _p$3.snapshot_index : _M0MPC15array5Array2atGsE(_p$3.log, _p$4 - 1 | 0).index)) {
      const _tmp$2 = self.id;
      const _p$5 = self.core;
      return [_M0MP28heke122814raft_2dmoonbit7Message3new(_tmp$2, target, new _M0DTP28heke122814raft_2dmoonbit7Payload10TimeoutNow(_p$5.current_term))];
    } else {
      return [_M0MP28heke122814raft_2dmoonbit8RaftNode8send__to(self, target)];
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Cluster20transfer__leadership(self, target) {
  const _bind = _M0MP28heke122814raft_2dmoonbit7Cluster6leader(self);
  if (_bind === undefined) {
    return false;
  } else {
    const _Some = _bind;
    const _id = _Some;
    const _bind$2 = _M0MP28heke122814raft_2dmoonbit8RaftNode20transfer__leadership(_M0MPB3Map2atGsRP28heke122814raft_2dmoonbit8RaftNodeE(self.nodes, _id), target);
    const _bind$3 = _bind$2.length;
    let _tmp = 0;
    while (true) {
      const _ = _tmp;
      if (_ < _bind$3) {
        const msg = _bind$2[_];
        _M0MP28heke122814raft_2dmoonbit7Cluster8schedule(self, msg);
        _tmp = _ + 1 | 0;
        continue;
      } else {
        break;
      }
    }
    return true;
  }
}
function _M0MP28heke122814raft_2dmoonbit7Message12is__response(self) {
  const _bind = self.payload;
  switch (_bind.$tag) {
    case 1: {
      return true;
    }
    case 3: {
      return true;
    }
    case 5: {
      return true;
    }
    case 7: {
      return true;
    }
    default: {
      return false;
    }
  }
}
function _M0MP28heke122814raft_2dmoonbit7Message4kind(self) {
  const _bind = self.payload;
  switch (_bind.$tag) {
    case 0: {
      return "PreVote";
    }
    case 1: {
      return "PreVoteResp";
    }
    case 2: {
      return "Vote";
    }
    case 3: {
      return "VoteResp";
    }
    case 4: {
      const _Append = _bind;
      const _a = _Append._0;
      return _a.entries.length === 0 ? "Heartbeat" : "Append";
    }
    case 5: {
      return "AppendResp";
    }
    case 6: {
      return "Snapshot";
    }
    case 7: {
      return "SnapshotResp";
    }
    default: {
      return "TimeoutNow";
    }
  }
}
function _M0FP28heke122814raft_2dmoonbit9demo__new(n, seed) {
  const count = n < 1 ? 1 : n > 9 ? 9 : n;
  const ids = [];
  let _tmp = 0;
  while (true) {
    const i = _tmp;
    if (i < count) {
      _M0MPC15array5Array4pushGsE(ids, `S${_M0MPC13int3Int18to__string_2einner(i + 1 | 0, 10)}`);
      _tmp = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster = _M0MP28heke122814raft_2dmoonbit7Cluster11new_2einner(ids, BigInt.asUintN(64, BigInt(seed)));
  _M0FP28heke122814raft_2dmoonbit9demo__ctx.seq = 0;
}
function _M0FP28heke122814raft_2dmoonbit10demo__tick() {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster4tick(_c);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit9demo__run(ticks) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster3run(_c, ticks);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit10cmd__bytes(s) {
  const arr = [];
  const _bind = s.length;
  let _tmp = 0;
  while (true) {
    const _string_index = _tmp;
    if (_string_index < _bind) {
      let _decoded_next_string_index;
      let _decoded_char;
      _L: {
        const _bind$2 = s.charCodeAt(_string_index);
        if (_bind$2 >= 55296 && _bind$2 <= 56319 && (_string_index + 1 | 0) < _bind) {
          const _bind$3 = s.charCodeAt(_string_index + 1 | 0);
          if (_bind$3 >= 56320 && _bind$3 <= 57343) {
            const _tmp$2 = _string_index + 2 | 0;
            const _p = (((Math.imul(_bind$2 - 55296 | 0, 1024) | 0) + _bind$3 | 0) - 56320 | 0) + 65536 | 0;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          } else {
            const _tmp$2 = _string_index + 1 | 0;
            const _p = _bind$2;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          }
        } else {
          const _tmp$2 = _string_index + 1 | 0;
          const _p = _bind$2;
          _decoded_next_string_index = _tmp$2;
          _decoded_char = _p;
          break _L;
        }
      }
      const code = _decoded_char;
      _M0MPC15array5Array4pushGyE(arr, code & 255);
      _M0MPC15array5Array4pushGyE(arr, code >> 8 & 255);
      _tmp = _decoded_next_string_index;
      continue;
    } else {
      break;
    }
  }
  return _M0MPC15bytes5Bytes11from__array(new _M0TPB9ArrayViewGyE(arr, 0, arr.length));
}
function _M0FP28heke122814raft_2dmoonbit13demo__propose(cmd) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return false;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0FP28heke122814raft_2dmoonbit9demo__ctx.seq = _M0FP28heke122814raft_2dmoonbit9demo__ctx.seq + 1 | 0;
    return _M0MP28heke122814raft_2dmoonbit7Cluster7propose(_c, _M0FP28heke122814raft_2dmoonbit10cmd__bytes(cmd));
  }
}
function _M0FP28heke122814raft_2dmoonbit17demo__propose__on(id, cmd) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return false;
  } else {
    const _Some = _bind;
    const _c = _Some;
    return _M0MP28heke122814raft_2dmoonbit7Cluster11propose__on(_c, id, _M0FP28heke122814raft_2dmoonbit10cmd__bytes(cmd));
  }
}
function _M0FP28heke122814raft_2dmoonbit13split__commas(s) {
  const out = [];
  let cur = "";
  const _bind = s.length;
  let _tmp = 0;
  while (true) {
    const _string_index = _tmp;
    if (_string_index < _bind) {
      let _decoded_next_string_index;
      let _decoded_char;
      _L: {
        const _bind$2 = s.charCodeAt(_string_index);
        if (_bind$2 >= 55296 && _bind$2 <= 56319 && (_string_index + 1 | 0) < _bind) {
          const _bind$3 = s.charCodeAt(_string_index + 1 | 0);
          if (_bind$3 >= 56320 && _bind$3 <= 57343) {
            const _tmp$2 = _string_index + 2 | 0;
            const _p = (((Math.imul(_bind$2 - 55296 | 0, 1024) | 0) + _bind$3 | 0) - 56320 | 0) + 65536 | 0;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          } else {
            const _tmp$2 = _string_index + 1 | 0;
            const _p = _bind$2;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          }
        } else {
          const _tmp$2 = _string_index + 1 | 0;
          const _p = _bind$2;
          _decoded_next_string_index = _tmp$2;
          _decoded_char = _p;
          break _L;
        }
      }
      if (_decoded_char === 44) {
        if (cur.length > 0) {
          _M0MPC15array5Array4pushGsE(out, cur);
        }
        cur = "";
      } else {
        cur = `${cur}${_M0IPC14char4CharPB4Show10to__string(_decoded_char)}`;
      }
      _tmp = _decoded_next_string_index;
      continue;
    } else {
      break;
    }
  }
  if (cur.length > 0) {
    _M0MPC15array5Array4pushGsE(out, cur);
  }
  return out;
}
function _M0FP28heke122814raft_2dmoonbit15demo__partition(group_a, group_b) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster9partition(_c, _M0FP28heke122814raft_2dmoonbit13split__commas(group_a), _M0FP28heke122814raft_2dmoonbit13split__commas(group_b));
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit13demo__isolate(id) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster7isolate(_c, id);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit10demo__heal() {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster4heal(_c);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit11demo__crash(id) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster5crash(_c, id);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit13demo__restart(id) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster7restart(_c, id);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit15demo__set__drop(permil) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster9set__drop(_c, permil);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit16demo__set__delay(ticks) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return;
  } else {
    const _Some = _bind;
    const _c = _Some;
    _M0MP28heke122814raft_2dmoonbit7Cluster10set__delay(_c, ticks);
    return;
  }
}
function _M0FP28heke122814raft_2dmoonbit14demo__transfer(id) {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return false;
  } else {
    const _Some = _bind;
    const _c = _Some;
    return _M0MP28heke122814raft_2dmoonbit7Cluster20transfer__leadership(_c, id);
  }
}
function _M0FP28heke122814raft_2dmoonbit5quote(s) {
  const b = _M0MPB13StringBuilder21StringBuilder_2einner(0);
  _M0IPB13StringBuilderPB6Logger11write__char(b, 34);
  const _bind = s.length;
  let _tmp = 0;
  while (true) {
    const _string_index = _tmp;
    if (_string_index < _bind) {
      let _decoded_next_string_index;
      let _decoded_char;
      _L: {
        const _bind$2 = s.charCodeAt(_string_index);
        if (_bind$2 >= 55296 && _bind$2 <= 56319 && (_string_index + 1 | 0) < _bind) {
          const _bind$3 = s.charCodeAt(_string_index + 1 | 0);
          if (_bind$3 >= 56320 && _bind$3 <= 57343) {
            const _tmp$2 = _string_index + 2 | 0;
            const _p = (((Math.imul(_bind$2 - 55296 | 0, 1024) | 0) + _bind$3 | 0) - 56320 | 0) + 65536 | 0;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          } else {
            const _tmp$2 = _string_index + 1 | 0;
            const _p = _bind$2;
            _decoded_next_string_index = _tmp$2;
            _decoded_char = _p;
            break _L;
          }
        } else {
          const _tmp$2 = _string_index + 1 | 0;
          const _p = _bind$2;
          _decoded_next_string_index = _tmp$2;
          _decoded_char = _p;
          break _L;
        }
      }
      if (_decoded_char === 34) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, "\\\"");
      } else {
        if (_decoded_char === 92) {
          _M0IPB13StringBuilderPB6Logger13write__string(b, "\\\\");
        } else {
          if (_decoded_char < 32) {
            _M0IPB13StringBuilderPB6Logger11write__char(b, 32);
          } else {
            _M0IPB13StringBuilderPB6Logger11write__char(b, _decoded_char);
          }
        }
      }
      _tmp = _decoded_next_string_index;
      continue;
    } else {
      break;
    }
  }
  _M0IPB13StringBuilderPB6Logger11write__char(b, 34);
  return b.val;
}
function _M0FP28heke122814raft_2dmoonbit10role__json(r) {
  switch (r) {
    case 0: {
      return "Follower";
    }
    case 1: {
      return "Candidate";
    }
    default: {
      return "Leader";
    }
  }
}
function _M0FP28heke122814raft_2dmoonbit18build__state__json(c) {
  const b = _M0MPB13StringBuilder21StringBuilder_2einner(0);
  _M0IPB13StringBuilderPB6Logger13write__string(b, "{\"ready\":true,\"tick\":");
  _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(c.now, 10));
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"n\":");
  _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(c.ids.length, 10));
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"dropPermil\":");
  _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(c.drop_permil, 10));
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"maxDelay\":");
  _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(c.max_delay, 10));
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"leaders\":[");
  const ls = _M0MP28heke122814raft_2dmoonbit7Cluster7leaders(c);
  const _bind = ls.length;
  let _tmp = 0;
  while (true) {
    const i = _tmp;
    if (i < _bind) {
      if (i > 0) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, ",");
      }
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_M0MPC15array5Array2atGsE(ls, i)));
      _tmp = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  _M0IPB13StringBuilderPB6Logger13write__string(b, "],\"leader\":");
  const _bind$2 = _M0MP28heke122814raft_2dmoonbit7Cluster6leader(c);
  if (_bind$2 === undefined) {
    _M0IPB13StringBuilderPB6Logger13write__string(b, "null");
  } else {
    const _Some = _bind$2;
    const _id = _Some;
    _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_id));
  }
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"invariants\":{\"electionSafety\":");
  const _p = _M0MP28heke122814raft_2dmoonbit7Cluster22one__leader__per__term(c);
  _M0IPB13StringBuilderPB6Logger13write__string(b, _p ? "true" : "false");
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"logMatching\":");
  const _p$2 = _M0MP28heke122814raft_2dmoonbit7Cluster16logs__consistent(c);
  _M0IPB13StringBuilderPB6Logger13write__string(b, _p$2 ? "true" : "false");
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"stateMachineSafety\":");
  const _p$3 = _M0MP28heke122814raft_2dmoonbit7Cluster17committed__agrees(c);
  _M0IPB13StringBuilderPB6Logger13write__string(b, _p$3 ? "true" : "false");
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"commandsAgree\":");
  const _p$4 = _M0MP28heke122814raft_2dmoonbit7Cluster25same__committed__commands(c);
  _M0IPB13StringBuilderPB6Logger13write__string(b, _p$4 ? "true" : "false");
  _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"allHold\":");
  const _p$5 = _M0MP28heke122814raft_2dmoonbit7Cluster16invariants__hold(c);
  _M0IPB13StringBuilderPB6Logger13write__string(b, _p$5 ? "true" : "false");
  _M0IPB13StringBuilderPB6Logger13write__string(b, "},\"nodes\":[");
  const _bind$3 = c.ids.length;
  let _tmp$2 = 0;
  while (true) {
    const i = _tmp$2;
    if (i < _bind$3) {
      if (i > 0) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, ",");
      }
      const id = _M0MPC15array5Array2atGsE(c.ids, i);
      const rn = _M0MP28heke122814raft_2dmoonbit7Cluster4node(c, id);
      const st = _M0MP28heke122814raft_2dmoonbit8RaftNode6status(rn);
      const core = rn.core;
      _M0IPB13StringBuilderPB6Logger13write__string(b, "{\"id\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(id));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"role\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_M0FP28heke122814raft_2dmoonbit10role__json(st.role)));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"term\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(st.term, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"commit\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(st.commit, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"lastIndex\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(st.last_index, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"applied\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(st.applied, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"down\":");
      const _p$6 = _M0MP28heke122814raft_2dmoonbit7Cluster8is__down(c, id);
      _M0IPB13StringBuilderPB6Logger13write__string(b, _p$6 ? "true" : "false");
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"group\":");
      const _bind$4 = _M0MPB3Map3getGsiE(c.part, id);
      let g;
      if (_bind$4 === undefined) {
        g = 0;
      } else {
        const _Some = _bind$4;
        g = _Some;
      }
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(g, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"leaderId\":");
      const _bind$5 = rn.leader_id;
      if (_bind$5 === undefined) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, "null");
      } else {
        const _Some = _bind$5;
        const _l = _Some;
        _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_l));
      }
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"votedFor\":");
      const _bind$6 = core.voted_for;
      if (_bind$6 === undefined) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, "null");
      } else {
        const _Some = _bind$6;
        const _v = _Some;
        _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_v));
      }
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"electionElapsed\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(rn.election_elapsed, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"randTimeout\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(rn.randomized_election_timeout, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"electionTimeout\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(rn.election_timeout, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"heartbeatElapsed\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(rn.heartbeat_elapsed, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"heartbeatTimeout\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(rn.heartbeat_timeout, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"log\":[");
      const log = core.log;
      const _bind$7 = log.length;
      let _tmp$3 = 0;
      while (true) {
        const j = _tmp$3;
        if (j < _bind$7) {
          if (j > 0) {
            _M0IPB13StringBuilderPB6Logger13write__string(b, ",");
          }
          const e = _M0MPC15array5Array2atGsE(log, j);
          _M0IPB13StringBuilderPB6Logger13write__string(b, "{\"i\":");
          _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(e.index, 10));
          _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"t\":");
          _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(e.term, 10));
          _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"c\":");
          const _p$7 = _M0MP28heke122814raft_2dmoonbit5Entry16is__conf__change(e);
          _M0IPB13StringBuilderPB6Logger13write__string(b, _p$7 ? "true" : "false");
          _M0IPB13StringBuilderPB6Logger13write__string(b, "}");
          _tmp$3 = j + 1 | 0;
          continue;
        } else {
          break;
        }
      }
      _M0IPB13StringBuilderPB6Logger13write__string(b, "]}");
      _tmp$2 = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  _M0IPB13StringBuilderPB6Logger13write__string(b, "],\"messages\":[");
  const _bind$4 = c.inflight.length;
  let _tmp$3 = 0;
  while (true) {
    const i = _tmp$3;
    if (i < _bind$4) {
      if (i > 0) {
        _M0IPB13StringBuilderPB6Logger13write__string(b, ",");
      }
      const f = _M0MPC15array5Array2atGsE(c.inflight, i);
      const m = f.msg;
      _M0IPB13StringBuilderPB6Logger13write__string(b, "{\"from\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(m.from));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"to\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(m.to));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"kind\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0FP28heke122814raft_2dmoonbit5quote(_M0MP28heke122814raft_2dmoonbit7Message4kind(m)));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"term\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC16uint646UInt6418to__string_2einner(_M0MP28heke122814raft_2dmoonbit7Message4term(m), 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"resp\":");
      const _p$6 = _M0MP28heke122814raft_2dmoonbit7Message12is__response(m);
      _M0IPB13StringBuilderPB6Logger13write__string(b, _p$6 ? "true" : "false");
      _M0IPB13StringBuilderPB6Logger13write__string(b, ",\"deliverAt\":");
      _M0IPB13StringBuilderPB6Logger13write__string(b, _M0MPC13int3Int18to__string_2einner(f.deliver_at, 10));
      _M0IPB13StringBuilderPB6Logger13write__string(b, "}");
      _tmp$3 = i + 1 | 0;
      continue;
    } else {
      break;
    }
  }
  _M0IPB13StringBuilderPB6Logger13write__string(b, "]}");
  return b.val;
}
function _M0FP28heke122814raft_2dmoonbit11demo__state() {
  const _bind = _M0FP28heke122814raft_2dmoonbit9demo__ctx.cluster;
  if (_bind === undefined) {
    return "{\"ready\":false}";
  } else {
    const _Some = _bind;
    const _c = _Some;
    return _M0FP28heke122814raft_2dmoonbit18build__state__json(_c);
  }
}
export { _M0FP28heke122814raft_2dmoonbit9demo__new as demo_new, _M0FP28heke122814raft_2dmoonbit10demo__tick as demo_tick, _M0FP28heke122814raft_2dmoonbit9demo__run as demo_run, _M0FP28heke122814raft_2dmoonbit13demo__propose as demo_propose, _M0FP28heke122814raft_2dmoonbit17demo__propose__on as demo_propose_on, _M0FP28heke122814raft_2dmoonbit15demo__partition as demo_partition, _M0FP28heke122814raft_2dmoonbit13demo__isolate as demo_isolate, _M0FP28heke122814raft_2dmoonbit10demo__heal as demo_heal, _M0FP28heke122814raft_2dmoonbit11demo__crash as demo_crash, _M0FP28heke122814raft_2dmoonbit13demo__restart as demo_restart, _M0FP28heke122814raft_2dmoonbit15demo__set__drop as demo_set_drop, _M0FP28heke122814raft_2dmoonbit16demo__set__delay as demo_set_delay, _M0FP28heke122814raft_2dmoonbit14demo__transfer as demo_transfer, _M0FP28heke122814raft_2dmoonbit11demo__state as demo_state }
