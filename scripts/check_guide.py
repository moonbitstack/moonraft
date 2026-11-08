"""Check that the guide page shows the code the executable guide actually runs.

`docs/quickstart.html` is the prose; `src/readme/GUIDE.mbt.md` is the same guide
as blocks `moon test` compiles and runs. The page's MoonBit blocks are copies of
those, so every one of them has to appear there verbatim, and every block of the
guide has to appear on the page — a snippet edited in one place and not the other
fails here instead of drifting.

Usage: python scripts/check_guide.py
"""

import html
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = ROOT / "docs" / "quickstart.html"
GUIDE = ROOT / "src" / "readme" / "GUIDE.mbt.md"

PRE = re.compile(r'<pre class="code mbt">(.*?)</pre>', re.S)
TAG = re.compile(r"<[^>]+>")
FENCE = re.compile(r"```mbt check\n(.*?)^```", re.S | re.M)


def page_blocks():
    text = PAGE.read_text(encoding="utf-8")
    return [html.unescape(TAG.sub("", m.group(1))).strip("\n") for m in PRE.finditer(text)]


def guide_blocks():
    text = GUIDE.read_text(encoding="utf-8")
    return [m.group(1).strip("\n") for m in FENCE.finditer(text)]


def main():
    shown = page_blocks()
    ran = guide_blocks()
    problems = []
    for block in shown:
        if block not in ran:
            first = block.splitlines()[0] if block else "(empty)"
            problems.append(f"{PAGE.name}: a block the guide does not run: {first}")
    for block in ran:
        if block not in shown:
            first = block.splitlines()[0] if block else "(empty)"
            problems.append(f"{GUIDE.name}: a block the page does not show: {first}")
    for p in problems:
        print(p)
    if problems:
        return 1
    print(f"quickstart.html: {len(shown)} blocks, all run by {GUIDE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
