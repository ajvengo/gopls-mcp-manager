#!/usr/bin/env python3
"""Enforce statement coverage per package and overall from a Go profile."""

import argparse
from collections import defaultdict
from pathlib import Path, PurePosixPath


def coverage(profile: str) -> dict[str, tuple[int, int]]:
    lines = profile.splitlines()
    if not lines or lines[0] not in {"mode: set", "mode: count", "mode: atomic"}:
        raise ValueError("expected a Go coverage profile")
    blocks = {}
    for line in lines[1:]:
        location, statements, hits = line.rsplit(maxsplit=2)
        statements, hits = int(statements), int(hits)
        if statements < 0 or hits < 0 or ":" not in location:
            raise ValueError("invalid coverage block")
        if location in blocks:
            prior_statements, prior_hits = blocks[location]
            if statements != prior_statements:
                raise ValueError("conflicting statement counts")
            hits += prior_hits
        blocks[location] = statements, hits
    totals = defaultdict(lambda: [0, 0])
    for location, (statements, hits) in blocks.items():
        package = str(PurePosixPath(location.rsplit(":", 1)[0]).parent)
        totals[package][0] += statements if hits else 0
        totals[package][1] += statements
    if not totals or not sum(total for _, total in totals.values()):
        raise ValueError("profile contains no statements")
    totals["TOTAL"] = [sum(n[0] for n in totals.values()), sum(n[1] for n in totals.values())]
    return {name: tuple(counts) for name, counts in totals.items()}


def check(profile: str, minimum: float) -> bool:
    passed = True
    for package, (covered, total) in sorted(coverage(profile).items()):
        percent = 100 * covered / total if total else 100.0
        ok = percent >= minimum
        print(f"{package}: {percent:.1f}% ({covered}/{total}) {'PASS' if ok else 'FAIL'}")
        passed = passed and ok
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("--minimum", type=float, default=85.0)
    args = parser.parse_args()
    if not 0 <= args.minimum <= 100:
        parser.error("minimum must be between 0 and 100")
    try:
        return 0 if check(args.profile.read_text(), args.minimum) else 1
    except (OSError, ValueError) as error:
        parser.exit(2, f"coverage check failed: {error}\n")


if __name__ == "__main__":
    raise SystemExit(main())
