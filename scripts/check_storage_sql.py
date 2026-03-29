#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
STORAGE_PATH = REPO_ROOT / "creatureos" / "storage.py"


def _split_top_level_csv(text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_quote: str | None = None
    i = 0
    while i < len(text):
        char = text[i]
        if in_quote:
            current.append(char)
            if char == in_quote:
                if i + 1 < len(text) and text[i + 1] == in_quote:
                    current.append(text[i + 1])
                    i += 1
                else:
                    in_quote = None
        else:
            if char in {"'", '"'}:
                in_quote = char
                current.append(char)
            elif char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth = max(0, depth - 1)
                current.append(char)
            elif char == "," and depth == 0:
                piece = "".join(current).strip()
                if piece:
                    parts.append(piece)
                current = []
            else:
                current.append(char)
        i += 1
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def main() -> int:
    source = STORAGE_PATH.read_text(encoding="utf-8")
    pattern = re.compile(
        r"INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)",
        re.IGNORECASE | re.DOTALL,
    )
    mismatches: list[str] = []
    checked = 0
    for match in pattern.finditer(source):
        table = match.group(1)
        columns = _split_top_level_csv(match.group(2))
        values = _split_top_level_csv(match.group(3))
        checked += 1
        if len(columns) != len(values):
            mismatches.append(
                f"{table}: {len(columns)} columns vs {len(values)} values at byte {match.start()}"
            )
    if mismatches:
        for item in mismatches:
            print(item, file=sys.stderr)
        return 1
    print(f"checked {checked} INSERT statements in {STORAGE_PATH.name}: all counts match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
