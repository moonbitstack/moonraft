"""Fill the counts in the README from the tree, and fail while they are stale.

The README states how many tests the suite runs and how many scripts it replays.
Both numbers move whenever a test or a script is added, so they are written into
the page between markers and rewritten here from the source rather than edited by
hand: `moon test` counts one `test` block as one case, and each archived script
is one file under `scripts/testdata/etcd/*/testdata/`.

    python scripts/gen_readme_numbers.py           # rewrite README.md
    python scripts/gen_readme_numbers.py --check   # exit 1 if README.md is stale
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
SRC = ROOT / "src"
SCRIPTS = ROOT / "scripts" / "testdata" / "etcd"

TEST = re.compile(r'^(?:async )?test "', re.M)
MARK = re.compile(r"(<!-- count:(tests|scripts) -->)\d+(<!-- /count -->)")


def counts():
    tests = 0
    for path in sorted(SRC.rglob("*.mbt")) + sorted(SRC.rglob("*.mbt.md")):
        tests += len(TEST.findall(path.read_text(encoding="utf-8")))
    scripts = [p for p in sorted(SCRIPTS.rglob("*.txt")) if p.parent.name == "testdata"]
    return {"tests": tests, "scripts": len(scripts)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    found = counts()
    before = README.read_text(encoding="utf-8")
    after = MARK.sub(lambda m: f"{m.group(1)}{found[m.group(2)]}{m.group(3)}", before)
    stale = before != after
    if args.check:
        if stale:
            print("README.md: the counts are stale; run scripts/gen_readme_numbers.py")
            return 1
        print(f"README.md: {found['tests']} tests, {found['scripts']} scripts, as in the tree")
        return 0
    if stale:
        README.write_text(after, encoding="utf-8", newline="\n")
        print(f"README.md: {found['tests']} tests, {found['scripts']} scripts")
    else:
        print(f"README.md: unchanged ({found['tests']} tests, {found['scripts']} scripts)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
