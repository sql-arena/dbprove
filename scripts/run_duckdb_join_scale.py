#!/usr/bin/env python3

import csv
import re
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DBPROVE = ROOT / "out/build/osx-arm-base/src/dbprove/dbprove"
SCALES = list(range(1, 11))
TIMEOUT_SECONDS = 120


def latest_proof_csv() -> Path | None:
    proof_root = ROOT / "proof" / "DuckDB"
    candidates = sorted(proof_root.glob("*/DuckDB_proof.csv"), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def parse_runtime_row(proof_csv: Path, theorem: str) -> dict[str, str] | None:
    with proof_csv.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="|", quotechar='"')
        rows_by_name: dict[str, str] = {}
        for row in reader:
            if row["THEOREM"] != theorem:
                continue
            rows_by_name[row["PROOF_NAME"]] = row["PROOF_VALUE"]
    return rows_by_name or None


def main() -> int:
    if not DBPROVE.exists():
        print(f"dbprove binary not found: {DBPROVE}", file=sys.stderr)
        return 1

    print("| Theorem | Status | Wall (s) | Avg (ms) | Min (ms) | Max (ms) |")
    print("| --- | --- | ---: | ---: | ---: | ---: |")

    for scale in SCALES:
        theorem = f"EE-JOIN-SCALE-{scale:02d}"
        cmd = [str(DBPROVE), "-e", "duckdb", "-T", theorem]
        start = time.time()
        try:
            proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            elapsed = time.time() - start
            print(f"| {theorem} | TIMEOUT | {elapsed:.2f} | - | - | - |")
            break

        elapsed = time.time() - start
        if proc.returncode != 0:
            print(f"| {theorem} | ERROR ({proc.returncode}) | {elapsed:.2f} | - | - | - |")
            stdout_tail = proc.stdout[-4000:]
            stderr_tail = proc.stderr[-4000:]
            if stdout_tail:
                print("\nstdout tail:\n")
                print(stdout_tail)
            if stderr_tail:
                print("\nstderr tail:\n")
                print(stderr_tail)
            return proc.returncode

        proof_csv = latest_proof_csv()
        runtime = parse_runtime_row(proof_csv, theorem) if proof_csv else None
        if runtime and {"RuntimeAvg", "RuntimeMin", "RuntimeMax"} <= runtime.keys():
            avg_ms = int(runtime["RuntimeAvg"]) / 1000.0
            min_ms = int(runtime["RuntimeMin"]) / 1000.0
            max_ms = int(runtime["RuntimeMax"]) / 1000.0
            print(f"| {theorem} | OK | {elapsed:.2f} | {avg_ms:.2f} | {min_ms:.2f} | {max_ms:.2f} |")
        else:
            print(f"| {theorem} | OK (no proof timing found) | {elapsed:.2f} | - | - | - |")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
