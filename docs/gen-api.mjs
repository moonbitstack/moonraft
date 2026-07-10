// Generate docs/api.html from pkg.generated.mbti — the authoritative public
// interface `moon info` emits, backed by the 362-test suite. Because `moon doc`
// crashes under the repo's rr-format moon.mod (it looks for a moon.mod.json that
// no longer exists), we render the .mbti directly instead. Run: node docs/gen-api.mjs
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const mbti = readFileSync(join(here, "..", "pkg.generated.mbti"), "utf8");
const lines = mbti.split(/\r?\n/);

const esc = (s) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

const types = new Map(); // name -> {name, kind, derive, fields:[], methods:[]}
function typeOf(name) {
  if (!types.has(name))
    types.set(name, { name, kind: "type", derive: "", fields: [], methods: [] });
  return types.get(name);
}

const values = [];
let mode = "";
for (let i = 0; i < lines.length; i++) {
  const line = lines[i];
  if (line.startsWith("// Values")) { mode = "values"; continue; }
  if (line.startsWith("// Errors")) { mode = "errors"; continue; }
  if (line.startsWith("// Types")) { mode = "types"; continue; }
  if (!line.trim() || line.startsWith("//") || line.startsWith("package"))
    continue;

  // a method on a type, wherever it appears
  let m = line.match(/^pub fn (\w+)::(\w+)\((.*)\) -> (.+)$/);
  if (m) {
    typeOf(m[1]).methods.push({ name: m[2], args: m[3], ret: m[4] });
    continue;
  }

  // a struct / enum block
  m = line.match(/^pub(\(all\))? (struct|enum) (\w+) ?\{/);
  if (m) {
    const t = typeOf(m[3]);
    t.kind = m[2];
    t.open = m[1] ? "all" : "abstract";
    for (i++; i < lines.length; i++) {
      const l = lines[i];
      const close = l.match(/^\} ?(derive\((.*)\))?/);
      if (close) {
        t.derive = close[2] || "";
        break;
      }
      if (l.trim() && !l.trim().startsWith("//")) t.fields.push(l.trim());
    }
    continue;
  }

  if (mode === "values") {
    let f = line.match(/^pub fn (\w+)\((.*)\) -> (.+)$/);
    if (f) { values.push({ kind: "fn", name: f[1], args: f[2], ret: f[3] }); continue; }
    let c = line.match(/^pub let (\w+) : (.+)$/);
    if (c) { values.push({ kind: "let", name: c[1], ret: c[2] }); continue; }
  }
}

// ---- grouping of free values ----
const bucket = (n) => {
  if (/^(demo_|wnode_)/.test(n)) return "Browser-demo bridge (FFI)";
  if (/^(run_election|replicate|run_single_node|committed_index|vote_result)$/.test(n))
    return "Synchronous drivers";
  return "Log &amp; entry helpers";
};
const valGroups = new Map();
for (const v of values) {
  const g = bucket(v.name);
  if (!valGroups.has(g)) valGroups.set(g, []);
  valGroups.get(g).push(v);
}

// ---- curated ordering for the type list ----
const featured = [
  "RaftNode", "RawNode", "Ready", "Message", "Cluster", "Node", "RaftStatus",
];
const blurbs = {
  RaftNode: "The message-driven consensus server: timers, campaigns, and the tick / step surface a transport drives.",
  RawNode: "The synchronous Ready/Advance driver over a RaftNode — the batch a real deployment persists, sends, and applies.",
  Ready: "One batch of outstanding work: state changes, entries to persist, committed entries to apply, and messages to send.",
  Message: "A routed message — a sender, a recipient, and a Payload. The unit a transport moves without interpreting.",
  Cluster: "The deterministic, single-seed simulator: drop, delay, reorder, partition, crash — with safety-invariant checks.",
  Node: "The consensus core state and role transitions the RaftNode wraps.",
  RaftStatus: "A point-in-time snapshot of a server's observable state, for monitoring and tests.",
};
const allNames = [...types.keys()].sort();
const ordered = [
  ...featured.filter((n) => types.has(n)),
  ...allNames.filter((n) => !featured.includes(n)),
];

const sig = (name, args, ret) =>
  `<code class="sig"><b>${esc(name)}</b>(${esc(args)}) <span class="arrow">→</span> ${esc(ret)}</code>`;

function typeCard(t) {
  const kindColor = t.kind === "enum" ? "candidate" : "follower";
  let h = `<section class="apitype card" id="t-${t.name}">`;
  h += `<div class="apihdr"><h3>${esc(t.name)}</h3><span class="chip ${kindColor}"><span class="dot"></span>${t.kind}</span>`;
  if (t.derive) h += `<span class="tag">derive(${esc(t.derive)})</span>`;
  h += `</div>`;
  if (blurbs[t.name]) h += `<p class="tblurb">${blurbs[t.name]}</p>`;
  if (t.fields.length) {
    const label = t.kind === "enum" ? "Variants" : "Fields";
    h += `<div class="tag">${label}</div><ul class="fields">`;
    for (const f of t.fields) h += `<li><code>${esc(f)}</code></li>`;
    h += `</ul>`;
  }
  if (t.methods.length) {
    h += `<div class="tag">Methods · ${t.methods.length}</div><ul class="methods">`;
    for (const mm of t.methods)
      h += `<li>${sig(mm.name, mm.args, mm.ret)}</li>`;
    h += `</ul>`;
  }
  h += `</section>`;
  return h;
}

let body = "";
// values first
body += `<section class="card" id="t-values"><div class="apihdr"><h3>Functions &amp; constants</h3></div>`;
for (const [g, vs] of valGroups) {
  body += `<div class="tag">${g} · ${vs.length}</div><ul class="methods">`;
  for (const v of vs) {
    body +=
      v.kind === "let"
        ? `<li><code class="sig"><b>${esc(v.name)}</b> : ${esc(v.ret)}</code></li>`
        : `<li>${sig(v.name, v.args, v.ret)}</li>`;
  }
  body += `</ul>`;
}
body += `</section>`;
for (const n of ordered) body += typeCard(types.get(n));

const toc =
  `<a href="#t-values">Functions</a>` +
  ordered.map((n) => `<a href="#t-${n}">${esc(n)}</a>`).join("");

const typeCount = types.size;
const methodCount = [...types.values()].reduce((a, t) => a + t.methods.length, 0);
const fnCount = values.length;

const page = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>API · raft-moonbit</title>
    <meta name="description" content="The public API of raft-moonbit, generated from the authoritative .mbti interface — ${typeCount} types, ${methodCount} methods, ${fnCount} free functions, all backed by 362 tests." />
    <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Ctext y='13' font-size='13'%3E%E2%9A%96%3C/text%3E%3C/svg%3E" />
    <link rel="stylesheet" href="assets/site.css" />
    <script src="assets/nav.js"></script>
  </head>
  <body>
    <header class="topbar"><div class="wrap row">
      <a class="brand" href="./"><span class="prompt">~/</span>raft-moonbit<span class="caret">▊</span></a>
      <nav class="links">
        <a href="./">Home</a><a href="quickstart.html">Quickstart</a>
        <a href="api.html">API</a><a href="demo.html">Demo</a><a href="design.html">Design</a>
      </nav>
      <span class="spacer"></span>
      <button class="iconbtn" data-theme-toggle></button>
      <button class="iconbtn" data-burger aria-label="Toggle menu">≡</button>
    </div></header>

    <main class="wrap">
      <section class="hero" style="padding:56px 0 24px">
        <p class="eyebrow">Reference</p>
        <h1 class="display" style="font-size:clamp(28px,4.6vw,46px)">The public API</h1>
        <p class="lede" style="font-size:16px">
          Generated straight from <code>pkg.generated.mbti</code> — the interface
          <code>moon info</code> emits and the 362-test suite pins.
          <b>${typeCount}</b> types · <b>${methodCount}</b> methods ·
          <b>${fnCount}</b> free functions.
        </p>
        <p class="note" style="margin-top:18px">
          Start with <a href="#t-RaftNode">RaftNode</a> (message-driven) or
          <a href="#t-Cluster">Cluster</a> (the batteries-included simulator).
          <a href="#t-RawNode">RawNode</a> is the Ready/Advance driver a real
          deployment runs.
        </p>
      </section>
      <div class="apilayout">
        <nav class="apitoc">${toc}</nav>
        <div class="apibody">${body}</div>
      </div>
    </main>

    <footer class="site"><div class="wrap row">
      <span>Generated from <code>pkg.generated.mbti</code> · Apache-2.0</span>
      <span class="mono">© 2026 Leo Cheng · <a href="https://github.com/Lfan-ke/raft-moonbit">GitHub</a></span>
    </div></footer>

    <style>
      .apilayout{display:grid;grid-template-columns:210px 1fr;gap:24px;align-items:start;padding-bottom:50px}
      .apitoc{position:sticky;top:76px;display:flex;flex-direction:column;gap:2px;max-height:calc(100vh - 96px);overflow-y:auto;font-family:var(--mono);font-size:12.5px}
      .apitoc a{color:var(--ink-2);padding:4px 8px;border-radius:6px}
      .apitoc a:hover{background:var(--surface-2);color:var(--ink);text-decoration:none}
      .apibody{display:flex;flex-direction:column;gap:16px;min-width:0}
      .apitype{scroll-margin-top:76px}
      .apihdr{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:6px}
      .apihdr h3{font-family:var(--mono);font-size:18px;margin:0;color:var(--ink)}
      .tblurb{color:var(--ink-2);font-size:14px;margin:4px 0 12px}
      .fields,.methods{list-style:none;margin:8px 0 16px;padding:0;display:flex;flex-direction:column;gap:5px}
      .fields li code{background:none;border:none;padding:0;color:var(--ink-2);font-size:13px}
      .methods li{overflow-x:auto}
      code.sig{background:none;border:none;padding:0;font-size:13px;color:var(--ink-2);white-space:nowrap}
      code.sig b{color:var(--ink);font-weight:640}
      code.sig .arrow{color:var(--replicate)}
      .apibody .tag{margin:10px 0 4px}
      @media (max-width:860px){.apilayout{grid-template-columns:1fr}.apitoc{position:static;flex-direction:row;flex-wrap:wrap;max-height:none}}
    </style>
  </body>
</html>
`;

writeFileSync(join(here, "api.html"), page);
console.log(`api.html: ${typeCount} types, ${methodCount} methods, ${fnCount} free values`);
