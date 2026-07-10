// Main thread: the network router and the visualization. It holds NO Raft state.
//
// Each node is a real Web Worker (node-worker.js) with its own wasm instance and
// its own timer. This file only (a) routes the flat-int messages the workers
// emit — dropping them across a partition, on random loss, or to a crashed node,
// and delivering the rest after an optional delay — and (b) draws whatever the
// nodes report about themselves. "Crash" is worker.terminate(); "restart" is a
// fresh Worker. The honest caveat: this is postMessage between threads on one
// machine, not a real network — but the concurrency and reordering are real.

const N = 5;
const SVGNS = "http://www.w3.org/2000/svg";
const KIND = {
  1: { name: "PreVote", cls: "vote" },
  2: { name: "PreVoteResp", cls: "voteResp" },
  3: { name: "Vote", cls: "vote" },
  4: { name: "VoteResp", cls: "voteResp" },
  5: { name: "Append", cls: "append" },
  6: { name: "AppendResp", cls: "appendResp" },
  7: { name: "Heartbeat", cls: "beat" },
  8: { name: "HeartbeatResp", cls: "appendResp" },
  9: { name: "TimeoutNow", cls: "transfer" },
};
const SPEEDS = { slow: 900, normal: 440, fast: 150 };

let workers = [];
let states = new Array(N).fill(null); // latest snapshot per node
let group = new Array(N).fill(0); // partition id per node
let down = new Set(); // crashed node indices
let inflight = []; // {from,to,kind,resp,t0,t1,id}
let msgId = 0;
let dropPermil = 0;
let delayScale = 0; // extra delay in units of tickMs
let tickMs = SPEEDS.normal;
let proposeSeq = 0;
let seedBase = 1000;
let stats = { sent: 0, dropped: 0, delivered: 0, elections: 0 };
let lastLeaderTerm = -1;

const el = (id) => document.getElementById(id);

// ---- worker lifecycle -------------------------------------------------------
function spawn(i, seed) {
  const w = new Worker(new URL("node-worker.js", import.meta.url), {
    type: "module",
  });
  w.onmessage = (e) => onWorker(i, e.data);
  w.postMessage({ t: "init", n: N, idx: i, seed, tickMs });
  workers[i] = w;
}

function resetCluster() {
  workers.forEach((w) => w && w.terminate());
  clearNetwork();
  workers = [];
  states = new Array(N).fill(null);
  group = new Array(N).fill(0);
  down = new Set();
  proposeSeq = 0;
  stats = { sent: 0, dropped: 0, delivered: 0, elections: 0 };
  lastLeaderTerm = -1;
  seedBase += 7919;
  for (let i = 0; i < N; i++) spawn(i, seedBase + i * 131);
  logEvent("cluster started — 5 nodes, each a wasm worker on its own timer");
}

function clearNetwork() {
  inflight.forEach((m) => clearTimeout(m.timer));
  inflight = [];
}

// ---- routing ----------------------------------------------------------------
function onWorker(i, m) {
  if (m.t === "state") {
    states[m.idx] = m.state;
    detectElection();
  } else if (m.t === "out") {
    for (const arr of m.msgs) route(arr);
  }
}

function route(arr) {
  const from = arr[1] - 1;
  const to = arr[2] - 1;
  if (to < 0 || to >= N) return;
  stats.sent++;
  if (down.has(from) || down.has(to)) {
    stats.dropped++;
    return;
  }
  if (group[from] !== group[to]) {
    stats.dropped++;
    return;
  }
  if (dropPermil > 0 && Math.random() * 1000 < dropPermil) {
    stats.dropped++;
    return;
  }
  const base = tickMs * 0.5;
  const extra = delayScale > 0 ? Math.random() * delayScale * tickMs : 0;
  const dur = base + extra;
  const now = performance.now();
  const meta = KIND[arr[0]] || { name: "?", cls: "beat" };
  const rec = {
    from,
    to,
    kind: arr[0],
    cls: meta.cls,
    t0: now,
    t1: now + dur,
    id: msgId++,
  };
  rec.timer = setTimeout(() => {
    const w = workers[to];
    if (w && !down.has(to) && group[from] === group[to]) {
      w.postMessage({ t: "recv", ints: arr });
      stats.delivered++;
    } else {
      stats.dropped++;
    }
    inflight = inflight.filter((x) => x.id !== rec.id);
  }, dur);
  inflight.push(rec);
}

function currentLeader() {
  // the leader of the highest term that a majority-visible node reports
  let best = null;
  for (let i = 0; i < N; i++) {
    const s = states[i];
    if (s && s.role === "Leader" && !down.has(i)) {
      if (!best || s.term > best.term) best = { idx: i, term: s.term };
    }
  }
  return best;
}

function detectElection() {
  const l = currentLeader();
  if (l && l.term !== lastLeaderTerm) {
    lastLeaderTerm = l.term;
    stats.elections++;
    logEvent(`S${l.idx + 1} became leader in term ${l.term}`);
  }
}

// ---- controls ---------------------------------------------------------------
function propose() {
  const l = currentLeader();
  if (!l) {
    logEvent("no leader right now — proposal dropped (as Raft would)");
    return;
  }
  proposeSeq++;
  workers[l.idx].postMessage({ t: "propose", cmd: proposeSeq });
  logEvent(`proposed cmd #${proposeSeq} to leader S${l.idx + 1}`);
}

function partitionSplit() {
  group = [0, 0, 1, 1, 1]; // {S1,S2} | {S3,S4,S5}
  logEvent("network split: {S1,S2} | {S3,S4,S5} — majority side re-elects");
}
function isolateLeader() {
  const l = currentLeader();
  if (!l) return;
  group = new Array(N).fill(0);
  group[l.idx] = 9;
  logEvent(`isolated leader S${l.idx + 1} — it can no longer reach a majority`);
}
function heal() {
  group = new Array(N).fill(0);
  logEvent("network healed — one shared network again; logs reconcile");
}
function crashLeader() {
  const l = currentLeader();
  if (!l) return;
  crash(l.idx);
}
function crash(i) {
  if (down.has(i)) return;
  workers[i].terminate();
  workers[i] = null;
  down.add(i);
  logEvent(`crashed S${i + 1} — its worker is gone; traffic to it is lost`);
}
function restartDown() {
  const i = [...down][0];
  if (i === undefined) return;
  down.delete(i);
  states[i] = null;
  spawn(i, seedBase + i * 131 + 3);
  logEvent(`restarted S${i + 1} — fresh node, catches up from the leader`);
}
function setSpeed(name) {
  tickMs = SPEEDS[name];
  workers.forEach((w) => w && w.postMessage({ t: "tickMs", ms: tickMs }));
}

// ---- rendering --------------------------------------------------------------
const CX = 300,
  CY = 250,
  R = 175;
function nodePos(i) {
  const a = (-90 + (i * 360) / N) * (Math.PI / 180);
  return { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) };
}

let ring, msgLayer;
function buildRing() {
  const svg = el("ring");
  svg.innerHTML = "";
  msgLayer = document.createElementNS(SVGNS, "g");
  const nodeLayer = document.createElementNS(SVGNS, "g");
  svg.appendChild(msgLayer);
  svg.appendChild(nodeLayer);
  ring = [];
  for (let i = 0; i < N; i++) {
    const p = nodePos(i);
    const g = document.createElementNS(SVGNS, "g");
    g.setAttribute("transform", `translate(${p.x},${p.y})`);
    const halo = mk("circle", { r: 40, class: "halo", fill: "none" });
    const c = mk("circle", { r: 30, class: "ncirc" });
    const id = mk("text", { class: "nid", y: 6 });
    id.textContent = "S" + (i + 1);
    const role = mk("text", { class: "nrole", y: 50 });
    g.append(halo, c, id, role);
    nodeLayer.appendChild(g);
    ring.push({ g, halo, c, id, role });
  }
}
function mk(tag, attrs) {
  const e = document.createElementNS(SVGNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}

function drawNodes() {
  for (let i = 0; i < N; i++) {
    const r = ring[i];
    const s = states[i];
    const isDown = down.has(i);
    let role = "follower";
    if (isDown) role = "down";
    else if (s) role = (s.role || "Follower").toLowerCase();
    r.c.setAttribute("class", "ncirc " + role);
    r.halo.setAttribute("class", "halo " + (role === "leader" ? "on" : ""));
    r.role.textContent = isDown
      ? "down"
      : s
        ? `${s.role.toLowerCase()} · t${s.term}`
        : "…";
    r.g.setAttribute("opacity", isDown ? 0.4 : 1);
  }
}

function drawMessages() {
  msgLayer.innerHTML = "";
  const now = performance.now();
  for (const m of inflight) {
    const a = nodePos(m.from),
      b = nodePos(m.to);
    const t = Math.max(0, Math.min(1, (now - m.t0) / (m.t1 - m.t0)));
    const x = a.x + (b.x - a.x) * t;
    const y = a.y + (b.y - a.y) * t;
    const line = mk("line", {
      x1: a.x,
      y1: a.y,
      x2: x,
      y2: y,
      class: "wire " + m.cls,
    });
    const dot = mk("circle", { cx: x, cy: y, r: 4, class: "pkt " + m.cls });
    msgLayer.append(line, dot);
  }
}

function drawLogs() {
  const box = el("logs");
  let maxLen = 1;
  for (const s of states) if (s && s.log) maxLen = Math.max(maxLen, s.lastIndex);
  maxLen = Math.min(maxLen, 16);
  let html = "";
  for (let i = 0; i < N; i++) {
    const s = states[i];
    const isDown = down.has(i);
    html += `<div class="logrow${isDown ? " dn" : ""}"><span class="lname">S${i + 1}</span><span class="lcells">`;
    if (s && s.log) {
      const byIdx = new Map(s.log.map((e) => [e.i, e]));
      for (let k = 1; k <= maxLen; k++) {
        const e = byIdx.get(k);
        if (!e) {
          html += `<i class="lc empty"></i>`;
        } else {
          const committed = k <= s.commit;
          html += `<i class="lc t${((e.t - 1) % 6) + 1}${committed ? " ok" : ""}" title="index ${k}, term ${e.t}">${e.t}</i>`;
        }
      }
      html += `</span><span class="lmeta">commit ${s.commit}/${s.lastIndex}</span>`;
    } else {
      html += `</span><span class="lmeta">${isDown ? "offline" : "…"}</span>`;
    }
    html += `</div>`;
  }
  box.innerHTML = html;
}

function checkInvariants() {
  const live = states
    .map((s, i) => ({ s, i }))
    .filter((x) => x.s && !down.has(x.i));
  // election safety: <= 1 leader per term
  const byTerm = new Map();
  let elecSafe = true;
  for (const { s } of live) {
    if (s.role === "Leader") {
      if (byTerm.has(s.term)) elecSafe = false;
      byTerm.set(s.term, true);
    }
  }
  // log matching + state-machine safety: committed prefixes agree on term
  let smSafe = true;
  const logs = live.map((x) => x.s);
  for (let a = 0; a < logs.length; a++) {
    for (let b = a + 1; b < logs.length; b++) {
      const hi = Math.min(logs[a].commit, logs[b].commit);
      const ma = new Map(logs[a].log.map((e) => [e.i, e.t]));
      const mb = new Map(logs[b].log.map((e) => [e.i, e.t]));
      for (let k = 1; k <= hi; k++) {
        if (ma.has(k) && mb.has(k) && ma.get(k) !== mb.get(k)) smSafe = false;
      }
    }
  }
  const leaders = live.filter((x) => x.s.role === "Leader");
  return {
    election: elecSafe,
    stateMachine: smSafe,
    leaders: leaders.map((x) => `S${x.i + 1}·t${x.s.term}`),
  };
}

function drawPanel() {
  const inv = checkInvariants();
  const set = (id, ok) => {
    const e = el(id);
    e.className = "inv " + (ok ? "ok" : "bad");
    e.querySelector(".v").textContent = ok ? "holds" : "VIOLATED";
  };
  set("inv-election", inv.election);
  set("inv-sm", inv.stateMachine);
  el("leaderNow").textContent = inv.leaders.length
    ? inv.leaders.join(", ")
    : "— none —";
  el("stat-sent").textContent = stats.sent;
  el("stat-dropped").textContent = stats.dropped;
  el("stat-elections").textContent = stats.elections;
  el("stat-inflight").textContent = inflight.length;
  const parts = new Set(group).size;
  el("stat-net").textContent =
    parts > 1 ? `${parts} partitions` : "connected";
}

let events = [];
function logEvent(msg) {
  const ts = new Date().toLocaleTimeString([], {
    hour12: false,
    minute: "2-digit",
    second: "2-digit",
  });
  events.unshift(`${ts}  ${msg}`);
  events = events.slice(0, 40);
  const box = el("events");
  if (box) box.textContent = events.join("\n");
}

// render loops
function frame() {
  drawNodes();
  drawMessages();
  requestAnimationFrame(frame);
}
setInterval(() => {
  drawLogs();
  drawPanel();
}, 180);

// ---- wire up ----------------------------------------------------------------
function bind() {
  el("btn-propose").onclick = propose;
  el("btn-reset").onclick = resetCluster;
  el("btn-split").onclick = partitionSplit;
  el("btn-isolate").onclick = isolateLeader;
  el("btn-heal").onclick = heal;
  el("btn-crash").onclick = crashLeader;
  el("btn-restart").onclick = restartDown;
  el("drop").oninput = (e) => {
    dropPermil = +e.target.value;
    el("drop-v").textContent = (dropPermil / 10).toFixed(0) + "%";
  };
  el("delay").oninput = (e) => {
    delayScale = +e.target.value;
    el("delay-v").textContent = delayScale ? `${delayScale}×` : "none";
  };
  document.querySelectorAll("[data-speed]").forEach((b) => {
    b.onclick = () => {
      document
        .querySelectorAll("[data-speed]")
        .forEach((x) => x.classList.remove("on"));
      b.classList.add("on");
      setSpeed(b.dataset.speed);
    };
  });
}

window.addEventListener("DOMContentLoaded", () => {
  buildRing();
  bind();
  resetCluster();
  requestAnimationFrame(frame);
});
