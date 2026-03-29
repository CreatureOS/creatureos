#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from creatureos import service


DEFAULT_BRIEF = "A steady companion who stays close and helps me keep going."


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview 10 generated creature names and the final choice for each ecosystem."
    )
    parser.add_argument(
        "brief",
        nargs="?",
        default=DEFAULT_BRIEF,
        help="Summoning brief to test. Defaults to a steady-companion brief.",
    )
    parser.add_argument(
        "--purpose-summary",
        default="",
        help="Optional explicit purpose summary to feed into naming.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of human-readable text.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.json:
        results: list[dict[str, object]] = []
        for ecosystem in service.ECOSYSTEMS:
            ecosystem_key = str(ecosystem["value"])
            preview = service.preview_summoning_names(
                brief=args.brief,
                ecosystem=ecosystem_key,
                purpose_summary=args.purpose_summary,
            )
            results.append(preview)
        print(json.dumps(results, indent=2))
        return 0

    print(f"Brief: {args.brief}\n")
    if args.purpose_summary:
        print(f"Purpose summary: {args.purpose_summary}\n")
    for ecosystem in service.ECOSYSTEMS:
        ecosystem_key = str(ecosystem["value"])
        result = service.preview_summoning_names(
            brief=args.brief,
            ecosystem=ecosystem_key,
            purpose_summary=args.purpose_summary,
        )
        label = str(result["ecosystem_label"])
        print(f"{label}")
        print(f"  Winner: {result['proposed_name']}")
        print(f"  Alternates: {', '.join(result['alternates']) or 'None'}")
        print("  Candidates:")
        candidates = list(result["candidates"])
        if not candidates:
            print("    (explicit name or fallback path)")
        else:
            for index, candidate in enumerate(candidates, start=1):
                print(f"    {index}. {candidate}")
        print()
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
