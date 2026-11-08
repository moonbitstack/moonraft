// Builds docs/api.html from the packages' pkg.generated.mbti.
//
// Every public item the compiler prints for a published package — a type, a
// method, a free function, a constant, a trait and its methods — has to reach the
// page, so the generator also checks that it did and fails when it did not.
// Run: node scripts/gen_api.mjs
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..", "src");

// Published packages, in dependency order. The root package is a façade that
// re-exports these, so its own .mbti carries no item that is not here already —
// the check at the end still walks it.
const pkgs = [
  "quorum", "tracker", "raftpb", "confchange", "storage", "log", "core",
  "moonlog_bridge",
];

const esc = (s) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

/**
 * One package's public surface.
 * @returns {{values: Array, types: Array, traits: Array, names: Array<string>}}
 */
function parse(file) {
  const lines = readFileSync(file, "utf8").split(/\r?\n/);
  const values = [];
  const types = [];
  const traits = [];
  const names = [];
  // `#derive(...)` sits on the line above the type it belongs to.
  let derive = "";
  let section = "";
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (line.startsWith("// Values")) { section = "values"; continue; }
    if (line.startsWith("// Errors")) { section = "errors"; continue; }
    if (line.startsWith("// Types and methods")) { section = "types"; continue; }
    if (line.startsWith("// Type aliases")) { section = "aliases"; continue; }
    if (line.startsWith("// Traits")) { section = "traits"; continue; }
    if (line.startsWith("//") || line.startsWith("package")) continue;
    if (!line.trim() || line === "}" || line.startsWith("import")) continue;

    if (line.startsWith("#derive")) { derive = line.slice(1); continue; }

    let m = line.match(/^pub fn (\w+)::(\w+)\((.*)\) -> (.+)$/);
    if (m) {
      const t = types.find((x) => x.name === m[1]);
      if (t) {
        t.methods.push({ name: m[2], args: m[3], ret: m[4] });
      } else {
        // A method of a type this package only re-exports: list it as a value.
        values.push({ kind: "fn", name: `${m[1]}::${m[2]}`, args: m[3], ret: m[4] });
      }
      names.push(m[2]);
      continue;
    }

    m = line.match(/^pub(?:\(all\))? (struct|enum|suberror) (\w+) ?\{/);
    if (m) {
      const t = {
        name: m[2],
        kind: m[1],
        derive,
        fields: [],
        methods: [],
        open: !line.includes("(all)") && m[1] === "struct" ? "abstract" : "all",
      };
      derive = "";
      for (i++; i < lines.length; i++) {
        const l = lines[i];
        if (l === "}") break;
        if (l.trim() && !l.trim().startsWith("//")) t.fields.push(l.trim());
      }
      types.push(t);
      names.push(t.name);
      continue;
    }

    m = line.match(/^pub\(open\) trait (\w+) \{/) || line.match(/^pub trait (\w+) \{/);
    if (m) {
      const t = { name: m[1], kind: "trait", methods: [], open: line.startsWith("pub(open)") };
      for (i++; i < lines.length; i++) {
        const l = lines[i];
        if (l === "}") break;
        const f = l.match(/^\s*fn (\w+)\((.*)\) -> (.+)$/);
        if (f) t.methods.push({ name: f[1], args: f[2], ret: f[3] });
      }
      traits.push(t);
      names.push(t.name);
      continue;
    }

    m = line.match(/^pub typealias (\w+)(.*)$/);
    if (m) {
      types.push({ name: m[1], kind: "typealias", derive: "", fields: [m[2].trim()], methods: [], open: "all" });
      names.push(m[1]);
      continue;
    }

    if (section === "values" || section === "errors") {
      let f = line.match(/^pub fn (\w+)\((.*)\) -> (.+)$/);
      if (f) { values.push({ kind: "fn", name: f[1], args: f[2], ret: f[3] }); names.push(f[1]); continue; }
      let c = line.match(/^pub let (\w+) : (.+)$/);
      if (c) { values.push({ kind: "let", name: c[1], ret: c[2] }); names.push(c[1]); continue; }
    }
  }
  return { values, types, traits, names };
}

const parsed = new Map();
for (const p of pkgs) {
  const f = join(root, p, "pkg.generated.mbti");
  parsed.set(p, existsSync(f) ? parse(f) : { values: [], types: [], traits: [], names: [] });
}

// The façade's own declarations: everything there must also be reachable.
const facade = existsSync(join(root, "pkg.generated.mbti"))
  ? parse(join(root, "pkg.generated.mbti"))
  : { names: [] };

// Free functions grouped by what they are for, within their package.
const bucket = (pkg, n) => {
  if (pkg === "core") {
    if (/^(demo_|wnode_)/.test(n)) return "Browser demo bridge";
    if (/^(run_election|replicate|run_single_node|committed_index|vote_result)$/.test(n)) return "Synchronous drivers";
    if (/^(describe|describe_)/.test(n)) return "Diagnostics & rendering";
    return "Entry, log & message helpers";
  }
  if (pkg === "quorum") return "Quorum arithmetic";
  if (pkg === "tracker") return "Progress & flow control";
  if (pkg === "raftpb") return "On-the-wire types & sizing";
  if (pkg === "confchange") return "Configurations & changes";
  if (pkg === "storage") return "Storage engines & the write-ahead log";
  if (pkg === "log") return "Raft log & unstable tail";
  if (pkg === "moonlog_bridge") return "Logging adapter";
  return "Values";
};

const featured = ["RaftNode", "RawNode", "Ready", "Message", "Cluster", "Node", "RaftStatus"];
const featureRank = (t) => {
  const i = featured.indexOf(t.name);
  return i < 0 ? featured.length : i;
};
const blurbs = {
  RaftNode: "The message-driven consensus server: timers, campaigns, and the tick / step surface a transport drives.",
  RawNode: "The synchronous Ready/Advance driver over a RaftNode — the batch a real deployment persists, sends, and applies.",
  Ready: "One batch of outstanding work: state changes, entries to persist, committed entries to apply, and messages to send.",
  Message: "A routed message — a sender, a recipient, and a Payload. The unit a transport moves without interpreting.",
  Cluster: "The deterministic, single-seed simulator: drop, delay, reorder, partition, crash — with safety-invariant checks.",
  Node: "The consensus core state and role transitions the RaftNode wraps.",
  RaftStatus: "A point-in-time snapshot of a server's observable state, for monitoring and tests.",
  Logger: "The one-method seam the core logs through. Defaults to silence; implement it and the diagnostics are yours to route.",
  StateMachine: "The only contract consensus has with your application: apply one committed command, in log order.",
  MoonlogLogger: "A core Logger that forwards every record to a moonlog logger, so a deployment keeps one logging model.",
};

const sig = (name, args, ret) =>
  `<code class="sig"><b>${esc(name)}</b>(${esc(args)}) <span class="arrow">→</span> ${esc(ret)}</code>`;

const kindColor = (kind) =>
  kind === "enum" ? "candidate" : kind === "trait" ? "leader" : kind === "typealias" ? "learner" : "follower";

function typeCard(t, pkg) {
  const fields = t.fields || [];
  const methods = t.methods || [];
  let h = `<section class="apitype card" id="t-${t.name}">`;
  h += `<div class="apihdr"><h3>${esc(t.name)}</h3><span class="chip ${kindColor(t.kind)}"><span class="dot"></span>${esc(t.kind)}</span><span class="tag">${esc(pkg)}</span>`;
  if (t.derive) h += `<span class="tag">${esc(t.derive)}</span>`;
  h += `</div>`;
  if (blurbs[t.name]) h += `<p class="tblurb">${blurbs[t.name]}</p>`;
  if (fields.length) {
    const label = t.kind === "enum" || t.kind === "suberror" ? "Variants" : "Fields";
    h += `<div class="tag">${label}</div><ul class="fields">`;
    for (const f of fields) h += `<li><code>${esc(f)}</code></li>`;
    h += `</ul>`;
  }
  if (methods.length) {
    h += `<div class="tag">${t.kind === "trait" ? "Required methods" : "Methods"} · ${methods.length}</div><ul class="methods">`;
    for (const mm of methods) h += `<li>${sig(mm.name, mm.args, mm.ret)}</li>`;
    h += `</ul>`;
  }
  h += `</section>\n`;
  return h;
}

let body = "";
let toc = `<a href="#t-values">Functions</a>`;
let typeCount = 0;
let methodCount = 0;
let fnCount = 0;

// ---- free values, grouped by package then by purpose ----
body += `<section class="card" id="t-values"><div class="apihdr"><h3>Functions &amp; constants</h3></div>`;
for (const pkg of pkgs) {
  const { values } = parsed.get(pkg);
  if (!values.length) continue;
  const groups = new Map();
  for (const v of values) {
    const g = bucket(pkg, v.name);
    if (!groups.has(g)) groups.set(g, []);
    groups.get(g).push(v);
  }
  for (const [g, vs] of groups) {
    body += `<div class="tag">${esc(pkg)} · ${esc(g)} · ${vs.length}</div>\n<ul class="methods">`;
    for (const v of vs) {
      if (v.kind === "let") {
        body += `<li><code class="sig"><b>${esc(v.name)}</b> : ${esc(v.ret)}</code></li>`;
      } else {
        body += `<li>${sig(v.name, v.args, v.ret)}</li>\n`;
      }
    }
    body += `</ul>`;
  }
  fnCount += values.length;
}
body += `</section>`;

// ---- traits first (they are what a third party implements), then the types a
// reader arrives for, then everything else package by package ----
const pkgRank = new Map(pkgs.map((p, i) => [p, i]));
const rows = [];
for (const pkg of pkgs) {
  const { traits, types } = parsed.get(pkg);
  for (const t of traits) rows.push({ t, pkg, group: 0 });
  for (const t of types) rows.push({ t, pkg, group: 1 });
}
rows.sort(
  (a, b) =>
    a.group - b.group ||
    featureRank(a.t) - featureRank(b.t) ||
    pkgRank.get(a.pkg) - pkgRank.get(b.pkg) ||
    a.t.name.localeCompare(b.t.name),
);
for (const { t, pkg } of rows) {
  body += typeCard(t, pkg);
  toc += `<a href="#t-${t.name}" class="toct">${esc(t.name)}</a>`;
  typeCount += 1;
  methodCount += (t.methods || []).length;
}

// ---- every public item of every published package must be on the page ----
const missing = [];
for (const [pkg, p] of parsed) {
  for (const n of p.names) {
    if (!body.includes(`>${esc(n)}<`) && !body.includes(`>${esc(n)}::`)) missing.push(`${pkg}: ${n}`);
  }
}
for (const n of facade.names) {
  if (!body.includes(`>${esc(n)}<`) && !body.includes(`>${esc(n)}::`)) missing.push(`raft (façade): ${n}`);
}
if (missing.length) {
  console.error(`api.html: ${missing.length} public item(s) missing from the page`);
  for (const m of missing.slice(0, 40)) console.error(`  ${m}`);
  process.exit(1);
}

const page = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>API · moonraft</title>
    <meta name="description" content="The public API of moonraft, generated from the authoritative .mbti interface — ${typeCount} types, ${methodCount} methods, ${fnCount} free functions across ${pkgs.length} packages." />
    <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Ctext y='13' font-size='13'%3E%E2%9A%96%3C/text%3E%3C/svg%3E" />
    <link rel="stylesheet" href="assets/site.css" />
    
    <script type="module" src="assets/scrollbar.js"></script><script src="assets/nav.js"></script>
  </head>
  <body>
    <header class="topbar"><div class="wrap row">
      <a class="brand" href="./"><span class="prompt">~/</span>moonraft<span class="caret">▊</span></a>
      <nav class="links">
        <a href="./">Home</a><a href="quickstart.html">Guide</a>
        <a href="api.html">API</a><a href="demo.html">Demo</a><a href="design.html">Design</a>
        <a href="porting.html">Porting</a><a href="ledger.html">Ledger</a>
        <a href="https://github.com/moonbitstack/moonraft" target="_blank" rel="noopener">GitHub</a>
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
          <code>moon info</code> emits and the public surface is pinned against.
          <b>${typeCount}</b> types and traits · <b>${methodCount}</b> methods ·
          <b>${fnCount}</b> free functions across ${pkgs.length} packages.
        </p>
        <p class="note" style="margin-top:18px">
          The root package is a façade: it re-exports these, so a consumer writes
          <code>@raft.X</code> whichever package <code>X</code> lives in. Start with
          <a href="#t-RaftNode">RaftNode</a> (message-driven), <a href="#t-Cluster">Cluster</a>
          (the batteries-included simulator) or <a href="#t-RawNode">RawNode</a> (the
          Ready/Advance driver a deployment runs).
        </p>
      </section>
      <div class="apilayout">
        <nav class="apitoc">${toc}</nav>
        <div class="apibody">${body}</div>
      </div>
    </main>

    <footer class="site"><div class="wrap row">
      <span>Generated from <code>pkg.generated.mbti</code> · Apache-2.0</span>
      <span class="mono">© 2026 Leo Cheng · <a href="https://github.com/moonbitstack/moonraft">GitHub</a></span>
    </div></footer>

    <style>
      .apilayout{display:grid;grid-template-columns:210px 1fr;gap:24px;align-items:start;padding-bottom:50px}
      .apitoc{position:sticky;top:76px;display:flex;flex-direction:column;gap:2px;max-height:calc(100vh - 96px);overflow-y:auto;font-family:var(--mono);font-size:12.5px}
      .apitoc a{color:var(--ink-2);padding:4px 8px;border-radius:6px}
      .apitoc a:hover{background:var(--surface-2);color:var(--ink);text-decoration:none}
      .apitoc a.toct{padding-left:16px}
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
      @media (max-width:860px){.apilayout{grid-template-columns:1fr}.apitoc{position:static;flex-direction:row;flex-wrap:wrap;max-height:none}.apitoc a.toct{padding-left:8px}}
    </style>
  </body>
</html>
`;

writeFileSync(join(here, "..", "docs", "api.html"), page);
console.log(`api.html: ${typeCount} types, ${methodCount} methods, ${fnCount} free values`);
