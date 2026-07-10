// One Raft node, running as a real Web Worker.
//
// This worker owns a single WebAssembly instance holding one server's RawNode
// (compiled from worker_driver.mbt). It ticks on its own setInterval timer, so
// the five nodes of the demo are genuinely concurrent and their message
// interleaving is non-deterministic — not a single-threaded animation. Nothing
// but small integers ever crosses into wasm; outbound traffic leaves as a JSON
// array of flat int arrays that the main thread routes.

let ex = null;
let idx = 0;
let n = 5;
let timer = null;
let tickMs = 460;
let lastStateAt = 0;

// Cap wasm-string allocation: MoonBit's linear-memory allocator does not free a
// returned String, so polling state every tick leaks. 220ms is frequent enough
// to look live and bounds growth to a few MB over a long session.
const STATE_MS = 220;

function readStr(ptr) {
  const mem = ex.memory.buffer;
  const len = new Uint32Array(mem, ptr - 4, 1)[0];
  return new TextDecoder("utf-16le").decode(
    new Uint8Array(mem, ptr, len * 2),
  );
}

function flush(force) {
  if (!ex) return;
  const out = JSON.parse(readStr(ex.wnode_drain()));
  if (out.length) postMessage({ t: "out", from: idx, msgs: out });
  const now = Date.now();
  if (force || now - lastStateAt >= STATE_MS) {
    lastStateAt = now;
    const st = JSON.parse(readStr(ex.wnode_state()));
    postMessage({ t: "state", idx, state: st });
  }
}

function startTimer() {
  if (timer) clearInterval(timer);
  timer = setInterval(() => {
    ex.wnode_tick();
    flush(false);
  }, tickMs);
}

async function boot(seed) {
  const url = new URL("../raft-moonbit.wasm", import.meta.url);
  const buf = await (await fetch(url)).arrayBuffer();
  const { instance } = await WebAssembly.instantiate(buf, {});
  ex = instance.exports;
  ex.wnode_new(n, idx, seed);
  postMessage({ t: "ready", idx });
  flush(true);
  startTimer();
}

onmessage = (e) => {
  const m = e.data;
  switch (m.t) {
    case "init":
      idx = m.idx;
      n = m.n;
      tickMs = m.tickMs || tickMs;
      boot(m.seed);
      break;
    case "recv":
      if (!ex) break;
      ex.wnode_recv_reset();
      for (const v of m.ints) ex.wnode_recv_push(v);
      ex.wnode_recv_apply();
      flush(false);
      break;
    case "propose":
      if (!ex) break;
      ex.wnode_propose(m.cmd);
      flush(true);
      break;
    case "campaign":
      if (!ex) break;
      ex.wnode_campaign();
      flush(true);
      break;
    case "tickMs":
      tickMs = m.ms;
      if (ex) startTimer();
      break;
  }
};
