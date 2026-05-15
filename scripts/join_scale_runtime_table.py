#!/usr/bin/env python3

import csv
import sys
from pathlib import Path


def scale_key(theorem_name: str) -> int:
    return int(theorem_name.rsplit("-", 1)[-1])


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {Path(sys.argv[0]).name} <proof_csv>", file=sys.stderr)
        return 1

    proof_csv = Path(sys.argv[1])
    if not proof_csv.exists():
        print(f"Proof CSV not found: {proof_csv}", file=sys.stderr)
        return 1

    rows_by_theorem: dict[str, dict[str, str]] = {}
    with proof_csv.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="|", quotechar='"')
        for row in reader:
            theorem = row["THEOREM"]
            if not theorem.startswith("EE-JOIN-SCALE-"):
                continue
            rows_by_theorem.setdefault(theorem, {})[row["PROOF_NAME"]] = row["PROOF_VALUE"]

    if not rows_by_theorem:
        print("No EE-JOIN-SCALE rows found.", file=sys.stderr)
        return 1

    print("| Theorem | Avg (ms) | Min (ms) | Max (ms) | Runs |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for theorem in sorted(rows_by_theorem, key=scale_key):
        row = rows_by_theorem[theorem]
        avg_ms = int(row["RuntimeAvg"]) / 1000.0
        min_ms = int(row["RuntimeMin"]) / 1000.0
        max_ms = int(row["RuntimeMax"]) / 1000.0
        runs = row["RuntimeRuns"]
        print(f"| {theorem} | {avg_ms:.2f} | {min_ms:.2f} | {max_ms:.2f} | {runs} |")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
