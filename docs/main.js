// Front end for the raft-moonbit browser demo.
//
// This file contains NO Raft logic. It imports the ES module emitted by
// `moon build --target js` and calls into the real MoonBit consensus code:
// demo_tick() advances the deterministic simulator one step, demo_state()
// returns a JSON snapshot, and the fault-injection functions (partition, crash,
// drop, ...) reach straight into the same Cluster the test-suite drives. All the
// visualization does is draw whatever demo_state() reports.

import * as raft from "./raft-moonbit.js";

// ---------- runtime state ----------
let state = null;            // latest parsed demo_state() snapshot
let prev = null;             // previous snapshot (for event detection)
let running = false;
let tickMs = 430;
let lastTickAt = 0;          // performance.now() at the last sim tick
let selected = null;         // selected node id
let seq = 0;                 // proposal counter (for distinct commands)
let scenarioActive = false;
let events = [];

// ---------- geometry ----------
const CX = 320, CY = 320, RING = 222, NR = 34;

function layout(nodes) {
  const m = new Map();
  const k = nodes.length;
  nodes.forEach((nd, i) => {
    const a = (-90 + (i * 360) / k) * Math.PI / 180;
    m.set(nd.id, { x: CX + RING * Math.cos(a), y: CY + RING * Math.sin(a) });
  });
  return m;
}

// ---------- colors ----------
const ROLE = { Leader: "#22d69a", Candidate: "#f6b73c", Follower: "#5b8cff" };
const DOWN = "#ff5470";
const TERM_COLORS = ["#5b8cff", "#22d69a", "#f6b73c", "#a78bfa", "#fb923c", "#f472b6", "#22d3ee", "#a3e635", "#fca5a5"];
function termColor(t) { const i = ((Number(t) - 1) % TERM_COLORS.length + TERM_COLORS.length) % TERM_COLORS.length; return TERM_COLORS[i]; }
function msgColor(kind) {
  switch (kind) {
    case "Vote": case "PreVote": return "#a78bfa";
    case "VoteResp": case "PreVoteResp": return "#c4b5fd";
    case "Append": return "#5b8cff";
    case "AppendResp": return "#34d399";
    case "Heartbeat": return "#5c6f8c";
    case "Snapshot": return "#fb923c";
    case "SnapshotResp": return "#fdba74";
    case "TimeoutNow": return "#f472b6";
    default: return "#8494ad";
  }
}

// ---------- svg helpers ----------
function polar(cx, cy, r, deg) { const a = (deg - 90) * Math.PI / 180; return [cx + r * Math.cos(a), cy + r * Math.sin(a)]; }
function arcPath(cx, cy, r, frac) {
  frac = Math.max(0, Math.min(0.999, frac));
  if (frac <= 0) return "";
  const end = 360 * frac;
  const [x0, y0] = polar(cx, cy, r, 0);
  const [x1, y1] = polar(cx, cy, r, end);
  const large = end > 180 ? 1 : 0;
  return `M ${x0.toFixed(1)} ${y0.toFixed(1)} A ${r} ${r} 0 ${large} 1 ${x1.toFixed(1)} ${y1.toFixed(1)}`;
}

// ---------- simulation driving ----------
function refresh() { prev = state; state = JSON.parse(raft.demo_state()); }

function simTick() {
  raft.demo_tick();
  refresh();
  detectEvents();
  lastTickAt = performance.now();
  renderPanels();
  renderMatrix();
}

function newCluster(n, seed) {
  raft.demo_new(n, seed);
  seq = 0; events = []; selected = null; prev = null;
  state = JSON.parse(raft.demo_state());
  lastTickAt = performance.now();
  logEvent(`new cluster · ${n} nodes · seed ${seed}`, "hi");
  updateSel();
  renderPanels();
  renderMatrix();
}

function reachable(a, b, byId) {
  const na = byId.get(a), nb = byId.get(b);
  if (!na || !nb) return false;
  if (na.down || nb.down) return false;
  return na.group === nb.group;
}

// ---------- event log ----------
function logEvent(msg, cls) {
  const t = state ? state.tick : 0;
  events.unshift({ t, msg, cls: cls || "" });
  if (events.length > 60) events.pop();
  const el = document.getElementById("events");
  if (el) el.innerHTML = events.map(e =>
    `<div class="ev ${e.cls}"><span class="t">[t${e.t}]</span> ${e.msg}</div>`).join("");
}

function detectEvents() {
  if (!prev || !state) return;
  if (prev.leader !== state.leader) {
    if (state.leader) {
      const nd = state.nodes.find(n => n.id === state.leader);
      logEvent(`${state.leader} elected leader — term ${nd ? nd.term : "?"}`, "good");
    } else {
      logEvent(`leader lost — cluster is electing`, "warn");
    }
  }
  const c0 = Math.max(0, ...prev.nodes.map(n => n.commit));
  const c1 = Math.max(0, ...state.nodes.map(n => n.commit));
  if (c1 > c0) logEvent(`committed up to index ${c1}`, "");
  const l0 = prev.leaders.length, l1 = state.leaders.length;
  if (l1 >= 2 && l0 < 2) logEvent(`split-brain: ${state.leaders.join(", ")} lead different terms`, "warn");
  if (!prev.invariants.electionSafety || !prev.invariants.stateMachineSafety) {
    // (only fires on a genuine violation, which must never happen)
  }
  if ((!state.invariants.electionSafety || !state.invariants.stateMachineSafety || !state.invariants.commandsAgree))
    logEvent(`SAFETY VIOLATION`, "bad");
}

// ---------- rendering: SVG network ----------
const net = document.getElementById("net");

function renderSVG(now) {
  if (!state || !state.ready) { net.innerHTML = ""; return; }
  const byId = new Map(state.nodes.map(n => [n.id, n]));
  const pos = layout(state.nodes);
  const p = tickMs > 0 ? Math.max(0, Math.min(1, (now - lastTickAt) / tickMs)) : 1;
  let s = "";

  // defs: soft glow for the leader
  s += `<defs><filter id="glow" x="-60%" y="-60%" width="220%" height="220%">
    <feGaussianBlur stdDeviation="5" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter></defs>`;

  // edges between every pair
  const ids = state.nodes.map(n => n.id);
  for (let i = 0; i < ids.length; i++) {
    for (let j = i + 1; j < ids.length; j++) {
      const A = pos.get(ids[i]), B = pos.get(ids[j]);
      const na = byId.get(ids[i]), nb = byId.get(ids[j]);
      if (na.down || nb.down) {
        s += `<line x1="${A.x}" y1="${A.y}" x2="${B.x}" y2="${B.y}" stroke="#16202f" stroke-width="1" stroke-dasharray="2 5"/>`;
      } else if (na.group !== nb.group) {
        s += `<line x1="${A.x}" y1="${A.y}" x2="${B.x}" y2="${B.y}" stroke="#ff547066" stroke-width="1.5" stroke-dasharray="5 6"/>`;
      } else {
        s += `<line x1="${A.x}" y1="${A.y}" x2="${B.x}" y2="${B.y}" stroke="#1c2c44" stroke-width="1"/>`;
      }
    }
  }

  // in-flight messages
  for (const m of state.messages) {
    const A = pos.get(m.from), B = pos.get(m.to);
    if (!A || !B) continue;
    const ok = reachable(m.from, m.to, byId);
    const col = msgColor(m.kind);
    const tp = ok ? p : Math.min(p, 0.46);
    const x = A.x + (B.x - A.x) * tp, y = A.y + (B.y - A.y) * tp;
    const r = (m.kind === "Heartbeat") ? 3.4 : 5.2;
    const op = ok ? 0.95 : Math.max(0, 0.9 - p);
    if (!ok) {
      s += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" fill="#ff5470" opacity="${op.toFixed(2)}"/>`;
      s += `<text x="${x.toFixed(1)}" y="${(y - 9).toFixed(1)}" text-anchor="middle" font-size="9" fill="#ff8098" opacity="${op.toFixed(2)}">✕</text>`;
    } else {
      s += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r + 2}" fill="${col}" opacity="0.18"/>`;
      s += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" fill="${col}" opacity="${op}"/>`;
    }
  }

  // nodes
  for (const nd of state.nodes) {
    const P = pos.get(nd.id);
    const isLeader = nd.role === "Leader" && !nd.down;
    const col = nd.down ? DOWN : (ROLE[nd.role] || "#5b8cff");
    const g = `<g data-node="${nd.id}" style="cursor:pointer">`;
    let body = g;

    if (selected === nd.id)
      body += `<circle cx="${P.x}" cy="${P.y}" r="${NR + 9}" fill="none" stroke="#ffffff" stroke-opacity=".5" stroke-width="1.5" stroke-dasharray="3 3"/>`;

    // countdown / heartbeat progress ring
    const ringR = NR + 5;
    let frac = 0, ringCol = "#2a3c57";
    if (!nd.down) {
      if (isLeader) { frac = nd.heartbeatTimeout ? nd.heartbeatElapsed / nd.heartbeatTimeout : 0; ringCol = "#1e6f57"; }
      else { frac = nd.randTimeout ? nd.electionElapsed / nd.randTimeout : 0; ringCol = nd.role === "Candidate" ? "#8a5a12" : "#274063"; }
    }
    body += `<circle cx="${P.x}" cy="${P.y}" r="${ringR}" fill="none" stroke="#131e2e" stroke-width="3"/>`;
    const ap = arcPath(P.x, P.y, ringR, frac);
    if (ap) body += `<path d="${ap}" fill="none" stroke="${ringCol}" stroke-width="3" stroke-linecap="round"/>`;

    // node disc
    const discFill = nd.down ? "#1a1016" : `${col}22`;
    body += `<circle cx="${P.x}" cy="${P.y}" r="${NR}" fill="${discFill}" stroke="${col}" stroke-width="${isLeader ? 3 : 1.8}" ${isLeader ? 'filter="url(#glow)"' : ""} ${nd.down ? 'stroke-dasharray="4 4"' : ""}/>`;

    if (isLeader)
      body += `<text x="${P.x}" y="${P.y - NR - 12}" text-anchor="middle" font-size="13" fill="#22d69a">★ leader</text>`;

    // id + term
    body += `<text x="${P.x}" y="${P.y - 2}" text-anchor="middle" font-size="17" font-weight="700" fill="${nd.down ? "#7d5563" : "#eaf1fb"}" font-family="ui-monospace,monospace">${nd.id}</text>`;
    body += `<text x="${P.x}" y="${P.y + 15}" text-anchor="middle" font-size="10.5" fill="${nd.down ? "#8a6070" : "#9fb0c9"}" font-family="ui-monospace,monospace">t${nd.term} · c${nd.commit}</text>`;

    if (nd.down)
      body += `<text x="${P.x}" y="${P.y + NR + 16}" text-anchor="middle" font-size="10" fill="#ff5470">crashed</text>`;

    body += `</g>`;
    s += body;
  }

  net.innerHTML = s;
}

// ---------- rendering: side panels ----------
function setInv(id, ok, label) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.remove("ok", "warn", "bad");
  el.classList.add(ok === true ? "ok" : (ok === "warn" ? "warn" : "bad"));
  el.querySelector(".state").textContent = label;
}

function renderPanels() {
  if (!state || !state.ready) return;
  const up = state.nodes.filter(n => !n.down).length;
  const term = Math.max(0, ...state.nodes.map(n => n.term));
  const commit = Math.max(0, ...state.nodes.filter(n => !n.down).map(n => n.commit));
  const leaderTxt = state.leaders.length > 1 ? state.leaders.join("/") : (state.leader || "—");

  document.getElementById("clk").textContent = state.tick;
  document.getElementById("clkLeader").textContent = leaderTxt;
  document.getElementById("sLeader").textContent = leaderTxt;
  document.getElementById("sTerm").textContent = term;
  document.getElementById("sCommit").textContent = commit;
  document.getElementById("sUp").textContent = `${up}/${state.nodes.length}`;

  const iv = state.invariants;
  setInv("invElection", iv.electionSafety, iv.electionSafety ? "holds" : "VIOLATED");
  setInv("invSM", iv.stateMachineSafety, iv.stateMachineSafety ? "holds" : "VIOLATED");
  setInv("invCmd", iv.commandsAgree, iv.commandsAgree ? "holds" : "VIOLATED");
  setInv("invConv", iv.logMatching ? true : "warn", iv.logMatching ? "converged" : "converging…");
}

// ---------- rendering: log matrix ----------
function renderMatrix() {
  const tbl = document.getElementById("matrix");
  if (!state || !state.ready) { tbl.innerHTML = ""; return; }
  const maxIdx = Math.max(0, ...state.nodes.map(n => n.lastIndex));
  if (maxIdx === 0) {
    tbl.innerHTML = `<tr><td style="color:var(--dim);padding:14px 4px;font-family:var(--sans)">No log entries yet — propose one, or run a scenario.</td></tr>`;
    return;
  }
  const from = Math.max(1, maxIdx - 23);
  let head = `<tr><th class="rowhead"></th>`;
  for (let i = from; i <= maxIdx; i++) head += `<th class="colhead">${i}</th>`;
  head += `</tr>`;

  let rows = "";
  for (const nd of state.nodes) {
    const rc = nd.down ? DOWN : (ROLE[nd.role] || "#5b8cff");
    const byIdx = new Map(nd.log.map(e => [e.i, e]));
    rows += `<tr><td class="rowhead" style="color:${rc}">${nd.id}${nd.down ? " ✕" : ""}</td>`;
    for (let i = from; i <= maxIdx; i++) {
      const e = byIdx.get(i);
      if (!e) { rows += `<td><div class="cell empty"></div></td>`; continue; }
      const tc = termColor(e.t);
      const committed = i <= nd.commit;
      const conf = e.c ? " confbadge" : "";
      if (committed)
        rows += `<td><div class="cell committed${conf}" style="background:${tc}">${e.t}</div></td>`;
      else
        rows += `<td><div class="cell uncommitted${conf}" style="border-color:${tc};color:${tc}">${e.t}</div></td>`;
    }
    rows += `</tr>`;
  }
  tbl.innerHTML = head + rows;
}

// ---------- main loop ----------
function loop(now) {
  if (running && !scenarioActive && now - lastTickAt >= tickMs) simTick();
  renderSVG(now);
  requestAnimationFrame(loop);
}

// ---------- controls ----------
function setPlay(on) {
  running = on;
  const b = document.getElementById("btnPlay");
  b.textContent = on ? "⏸ Pause" : "▶ Run";
  b.classList.toggle("primary", !on);
}
function updateSel() {
  document.getElementById("selName").textContent = selected ? selected : "— (click a node)";
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

function currentN() { return Number(document.querySelector("#segSize button.on").dataset.n); }
function currentSeed() { return Number(document.getElementById("inSeed").value) || 0; }

function propose() {
  seq++;
  const ok = raft.demo_propose("v" + seq);
  logEvent(ok ? `proposed v${seq} → leader accepted` : `propose v${seq} — no leader`, ok ? "" : "warn");
  refresh(); renderPanels(); renderMatrix();
}

function splitBrain() {
  if (!state) return;
  const ids = state.nodes.map(n => n.id);
  const L = state.leader || ids[0];
  const others = ids.filter(x => x !== L);
  const minorityExtra = Math.max(0, Math.ceil(ids.length / 2) - 2);
  const groupA = [L, ...others.slice(0, minorityExtra)];
  const groupB = ids.filter(x => !groupA.includes(x));
  raft.demo_partition(groupA.join(","), groupB.join(","));
  logEvent(`partition · {${groupA.join(",")}} ⇋ {${groupB.join(",")}}`, "warn");
  refresh(); renderPanels();
}

function wire() {
  document.getElementById("btnPlay").onclick = () => setPlay(!running);
  document.getElementById("btnStep").onclick = () => { setPlay(false); simTick(); };
  document.getElementById("btnReset").onclick = () => { newCluster(currentN(), currentSeed()); setPlay(true); };

  document.getElementById("segSpeed").addEventListener("click", e => {
    const b = e.target.closest("button"); if (!b) return;
    document.querySelectorAll("#segSpeed button").forEach(x => x.classList.remove("on"));
    b.classList.add("on"); tickMs = Number(b.dataset.ms);
  });
  document.getElementById("segSize").addEventListener("click", e => {
    const b = e.target.closest("button"); if (!b) return;
    document.querySelectorAll("#segSize button").forEach(x => x.classList.remove("on"));
    b.classList.add("on"); newCluster(currentN(), currentSeed());
  });
  document.getElementById("inSeed").onchange = () => newCluster(currentN(), currentSeed());

  document.getElementById("btnPropose").onclick = propose;
  document.getElementById("btnProp5").onclick = () => { for (let i = 0; i < 5; i++) propose(); };

  document.getElementById("btnCrash").onclick = () => {
    if (!selected) return logEvent("select a node first", "warn");
    raft.demo_crash(selected); logEvent(`crashed ${selected}`, "bad"); refresh(); renderPanels(); renderMatrix();
  };
  document.getElementById("btnRestart").onclick = () => {
    if (!selected) return logEvent("select a node first", "warn");
    raft.demo_restart(selected); logEvent(`restarted ${selected}`, "good"); refresh(); renderPanels(); renderMatrix();
  };
  document.getElementById("btnIsolate").onclick = () => {
    if (!selected) return logEvent("select a node first", "warn");
    raft.demo_isolate(selected); logEvent(`isolated ${selected}`, "warn"); refresh(); renderPanels();
  };
  document.getElementById("btnSplit").onclick = splitBrain;
  document.getElementById("btnHeal").onclick = () => { raft.demo_heal(); logEvent("healed network", "good"); refresh(); renderPanels(); };

  const inDrop = document.getElementById("inDrop"), vDrop = document.getElementById("vDrop");
  inDrop.oninput = () => { raft.demo_set_drop(Number(inDrop.value)); vDrop.textContent = inDrop.value + "‰"; };
  const inDelay = document.getElementById("inDelay"), vDelay = document.getElementById("vDelay");
  inDelay.oninput = () => { raft.demo_set_delay(Number(inDelay.value)); vDelay.textContent = inDelay.value + "t"; };

  // node selection (delegated; #net persists across re-renders)
  net.addEventListener("click", e => {
    const g = e.target.closest("[data-node]");
    if (g) { selected = g.getAttribute("data-node"); updateSel(); }
  });

  document.getElementById("scElect").onclick = () => runScenario(scElection);
  document.getElementById("scSplit").onclick = () => runScenario(scSplitHeal);
  document.getElementById("scCrash").onclick = () => runScenario(scLeaderCrash);
  document.getElementById("scChaos").onclick = () => runScenario(scChaos);
}

// ---------- scenarios ----------
async function runScenario(fn) {
  if (scenarioActive) return;
  scenarioActive = true; setPlay(false);
  try { await fn(); } finally { scenarioActive = false; }
}
function scMs() { return Math.max(130, Math.min(tickMs, 300)); }
async function tickN(k) { for (let i = 0; i < k; i++) { simTick(); await sleep(scMs()); } }
async function tickUntil(pred, max) { for (let i = 0; i < max; i++) { simTick(); await sleep(scMs()); if (pred(state)) return true; } return false; }

async function scElection() {
  newCluster(currentN(), currentSeed());
  logEvent("scenario · leader election", "hi");
  await sleep(300);
  await tickUntil(s => s.leader, 40);
  await tickN(6);
}
async function scSplitHeal() {
  newCluster(currentN(), currentSeed());
  logEvent("scenario · split-brain & heal", "hi");
  await tickUntil(s => s.leader, 40);
  seq++; raft.demo_propose("v" + seq); refresh();
  await tickN(14);
  splitBrain();
  const L = state.leaders[0];
  await sleep(300);
  seq++; if (L) raft.demo_propose_on(L, "stale" + seq);  // stale side cannot commit
  await tickN(28);
  const maj = state.leaders.find(x => x) ;
  seq++; raft.demo_propose("v" + seq); refresh();          // majority commits
  await tickN(16);
  raft.demo_heal(); logEvent("healed network", "good"); refresh();
  await tickN(30);
}
async function scLeaderCrash() {
  if (!state || !state.leader) { newCluster(currentN(), currentSeed()); await tickUntil(s => s.leader, 40); }
  logEvent("scenario · leader crash", "hi");
  await tickN(4);
  const L = state.leader; if (L) { raft.demo_crash(L); logEvent(`crashed leader ${L}`, "bad"); refresh(); }
  await tickUntil(s => s.leader && s.leader !== L, 50);
  await tickN(10);
  if (L) { raft.demo_restart(L); logEvent(`restarted ${L}`, "good"); refresh(); }
  await tickN(24);
}
async function scChaos() {
  logEvent("scenario · packet-loss storm (25% drop, jitter 3)", "hi");
  document.getElementById("inDrop").value = 250; raft.demo_set_drop(250); document.getElementById("vDrop").textContent = "250‰";
  document.getElementById("inDelay").value = 3; raft.demo_set_delay(3); document.getElementById("vDelay").textContent = "3t";
  for (let i = 0; i < 3; i++) { seq++; raft.demo_propose("v" + seq); }
  await tickN(70);
  raft.demo_set_drop(0); raft.demo_set_delay(0);
  document.getElementById("inDrop").value = 0; document.getElementById("vDrop").textContent = "0‰";
  document.getElementById("inDelay").value = 0; document.getElementById("vDelay").textContent = "0t";
  logEvent("network recovered", "good");
  for (let i = 0; i < 4; i++) { seq++; raft.demo_propose("v" + seq); }
  await tickN(24);
}

// ---------- boot ----------
wire();
newCluster(5, 7);
setPlay(true);
requestAnimationFrame(loop);
