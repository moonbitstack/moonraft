#!/usr/bin/env python3
# Field-level differ for the two normalized trace streams.
# Usage: diff.py go.jsonl mb.jsonl [label]
import json
import sys

NODE_FIELDS = ["role", "term", "vote", "lead", "commit", "applied", "last_index",
               "last_term", "lead_transferee", "pending_conf_index", "uncommitted",
               "voters", "voters_outgoing", "learners", "learners_next"]
MSG_FIELDS = ["type", "from", "to", "term", "index", "logterm", "entries",
              "commit", "reject", "rejecthint"]


def load(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def diffs(go, mb, label):
    rows = []
    n = max(len(go), len(mb))
    for i in range(n):
        g = go[i] if i < len(go) else None
        m = mb[i] if i < len(mb) else None
        if g is None:
            rows.append((i + 1, "<frame>", "<missing>", "MB-only:" + m["cmd"]))
            continue
        if m is None:
            rows.append((i + 1, "<frame>", "GO-only:" + g["cmd"], "<missing>"))
            continue
        step = g.get("step", i + 1)
        if g["cmd"] != m["cmd"]:
            rows.append((step, "cmd", g["cmd"], m["cmd"]))
            continue
        gn = {x["id"]: x for x in g["nodes"]}
        mn = {x["id"]: x for x in m["nodes"]}
        for nid in sorted(set(gn) | set(mn), key=lambda s: int(s)):
            if nid not in gn:
                rows.append((step, f"node{nid}", "<missing>", "present"))
                continue
            if nid not in mn:
                rows.append((step, f"node{nid}", "present", "<missing>"))
                continue
            for fld in NODE_FIELDS:
                gv, mv = gn[nid].get(fld), mn[nid].get(fld)
                if gv != mv:
                    rows.append((step, f"n{nid}.{fld}", gv, mv))
        gm, mm = g["msgs"], m["msgs"]
        if len(gm) != len(mm):
            rows.append((step, "msgs.count", len(gm), len(mm)))
        for j in range(min(len(gm), len(mm))):
            for fld in MSG_FIELDS:
                gv, mv = gm[j].get(fld), mm[j].get(fld)
                if gv != mv:
                    rows.append((step, f"msg[{j}].{fld}", gv, mv))
        # show the actual extra messages when counts differ
        if len(gm) != len(mm):
            longer = gm if len(gm) > len(mm) else mm
            side = "GO" if len(gm) > len(mm) else "MB"
            for j in range(min(len(gm), len(mm)), len(longer)):
                rows.append((step, f"msg[{j}]:{side}-only",
                             json.dumps(longer[j], sort_keys=True), ""))
    return rows


def main():
    go = load(sys.argv[1])
    mb = load(sys.argv[2])
    label = sys.argv[3] if len(sys.argv) > 3 else ""
    rows = diffs(go, mb, label)
    if not rows:
        print(f"MATCH  {label}  ({len(go)} frames)")
        return 0
    print(f"DIVERGE  {label}  ({len(rows)} field diffs over {len(go)}/{len(mb)} frames)")
    print(f"{'step':>4}  {'field':<26}  {'GO':<28}  MB")
    for step, fld, gv, mv in rows:
        print(f"{step:>4}  {str(fld):<26}  {str(gv):<28}  {mv}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
