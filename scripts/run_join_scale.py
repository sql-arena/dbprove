#!/usr/bin/env python3

import argparse
import csv
import os
import shlex
import subprocess
import sys
import time
from urllib.request import urlopen
from urllib.error import URLError
from math import ceil
from pathlib import Path

from matplotlib.ticker import MultipleLocator
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
PROOF_ROOT = ROOT / "proof"
QUERY_ROOT = PROOF_ROOT / "join_scale_queries"
PLOT_PATH = PROOF_ROOT / "join_scale_runtime.webp"
PARQUET_SOURCE_DIR = ROOT / "docker" / "datafusion" / "tpch" / "sf1"


def default_dbprove_path() -> Path:
    env_path = os.environ.get("DBPROVE_BIN")
    if env_path:
        return Path(env_path)

    release = ROOT / "out/build/osx-arm-release/src/dbprove/dbprove"
    if release.exists():
        return release

    return ROOT / "out/build/osx-arm-base/src/dbprove/dbprove"


DBPROVE = default_dbprove_path()
SCALES = list(range(1, 21))
QUERY_TIMEOUT_SECONDS = 30
TIMING_RUNS = 3
WARMUP_TIMEOUT_SECONDS = 1800
ENGINE_RUN_TIMEOUT_SECONDS = max(QUERY_TIMEOUT_SECONDS * len(SCALES) * 12, 600)
ENGINE_COLUMNS = ["DuckDB", "DataFusion", "Trino"]
DOCKER_COMPOSE_FILE = ROOT / "docker" / "docker-compose.yml"
DUCKDB_ARTIFACT_SCRIPT = ROOT / "scripts" / "build_duckdb_ubuntu_prebuilt_artifact.sh"
DUCKDB_IMAGE = "dbprove-duckdb-bench:latest"

ENGINES = [
    {"arg": "duckdb", "name": "DuckDB", "warmup": False},
    {"arg": "datafusion", "name": "DataFusion", "warmup": False, "compose_build_service": "datafusion"},
    {"arg": "trino", "name": "Trino", "warmup": True, "compose_build_service": "trino", "compose_service": "trino"},
]


def theorem_names(scales: list[int]) -> list[str]:
    return [f"EE-JOIN-SCALE-{scale:02d}" for scale in scales]


def scale_key(theorem_name: str) -> int:
    return int(theorem_name.rsplit("-", 1)[-1])


def latest_proof_csv(engine_name: str) -> Path | None:
    proof_root = PROOF_ROOT / engine_name
    candidates = sorted(proof_root.glob("*/" + f"{engine_name}_proof.csv"), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def parse_proof_rows(proof_csv: Path, theorems: list[str]) -> dict[str, dict[str, str]]:
    rows_by_theorem: dict[str, dict[str, str]] = {}
    with proof_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter="|", quotechar='"')
        for row in reader:
            theorem = row["THEOREM"]
            if theorem not in theorems:
                continue
            rows_by_theorem.setdefault(theorem, {})[row["PROOF_NAME"]] = row["PROOF_VALUE"]
    return rows_by_theorem


def has_complete_ok_runtime(row: dict[str, str] | None) -> bool:
    required_fields = {
        "RunStatus",
        "RuntimeRuns",
        "RuntimeBest",
        "RuntimeAvg",
        "RuntimeMin",
        "RuntimeMax",
        "RuntimeStdDev",
    }
    return row is not None and row.get("RunStatus") == "OK" and required_fields <= row.keys()


def collect_rows(theorems: list[str]) -> tuple[dict[str, dict[str, dict[str, str]]], dict[str, str]]:
    rows_by_engine: dict[str, dict[str, dict[str, str]]] = {}
    sql_by_theorem: dict[str, str] = {}
    for proof_csv in sorted(PROOF_ROOT.glob("*/*/*_proof.csv"), key=lambda path: path.stat().st_mtime):
        with proof_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="|", quotechar='"')
            for row in reader:
                theorem = row["THEOREM"]
                if theorem not in theorems:
                    continue
                engine = row["ENGINE"]
                rows_by_engine.setdefault(engine, {}).setdefault(theorem, {})[row["PROOF_NAME"]] = row["PROOF_VALUE"]
                if row["PROOF_NAME"] == "SQL" and theorem not in sql_by_theorem:
                    sql_by_theorem[theorem] = row["PROOF_VALUE"]
    return rows_by_engine, sql_by_theorem


def write_query_files(sql_by_theorem: dict[str, str]) -> dict[str, str]:
    QUERY_ROOT.mkdir(parents=True, exist_ok=True)
    query_paths: dict[str, str] = {}
    for theorem, sql_text in sql_by_theorem.items():
        query_path = QUERY_ROOT / f"{theorem}.sql"
        query_path.write_text(sql_text)
        query_paths[theorem] = query_path.relative_to(ROOT).as_posix()
    return query_paths


def first_non_ok_theorem(engine_rows: dict[str, dict[str, str]], theorems: list[str]) -> tuple[str, str] | None:
    for theorem in theorems:
        row = engine_rows.get(theorem)
        if row is None:
            continue
        status = row.get("RunStatus")
        if status in {"ERROR", "TIMEOUT"}:
            message = row.get("ErrorMessage", "Execution failed").splitlines()[0].strip()
            return theorem, f"{status.lower()}: {message}"
        if status is None:
            proof_names = sorted(row.keys())
            last_proof = proof_names[-1] if proof_names else "unknown"
            return theorem, f"incomplete proof output (last field: {last_proof})"
    return None


def first_missing_theorem_after_success(engine_rows: dict[str, dict[str, str]], theorems: list[str]) -> str | None:
    seen_success = False
    for theorem in theorems:
        row = engine_rows.get(theorem)
        if row and row.get("RunStatus") == "OK":
            seen_success = True
            continue
        if seen_success and row is None:
            return theorem
    return None


def render_plot(rows_by_engine: dict[str, dict[str, dict[str, str]]], theorems: list[str]) -> Path | None:
    all_scales = sorted({scale_key(theorem) for theorem in theorems})
    min_scale = min(all_scales)
    max_scale = max(all_scales)

    series_by_engine: dict[str, tuple[list[int], list[float]]] = {}
    for engine in ENGINE_COLUMNS:
        scales: list[int] = []
        runtimes_ms: list[float] = []
        for theorem in theorems:
            runtime = rows_by_engine.get(engine, {}).get(theorem)
            if has_complete_ok_runtime(runtime):
                scales.append(scale_key(theorem))
                runtimes_ms.append(int(runtime["RuntimeBest"]) / 1000.0)
        if scales:
            series_by_engine[engine] = (scales, runtimes_ms)

    if not series_by_engine:
        return None

    colours = {
        "DuckDB": "#c55a11",
        "DataFusion": "#1a73e8",
        "Trino": "#1b8754",
    }

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(11, 7), constrained_layout=True)
    fig.patch.set_facecolor("#faf8f3")
    ax.set_facecolor("#faf8f3")

    no_series_annotations: list[tuple[str, str]] = []

    for engine in ENGINE_COLUMNS:
        engine_rows = rows_by_engine.get(engine, {})
        annotation_target: tuple[int, str] | None = None

        non_ok = first_non_ok_theorem(engine_rows, theorems)
        if non_ok is not None:
            annotation_target = (scale_key(non_ok[0]), non_ok[1])
        else:
            missing_theorem = first_missing_theorem_after_success(engine_rows, theorems)
            if missing_theorem is not None:
                annotation_target = (scale_key(missing_theorem), "no proof rows written")

        if engine not in series_by_engine:
            if annotation_target is not None:
                failed_scale, annotation = annotation_target
                if len(annotation) > 72:
                    annotation = annotation[:69] + "..."
                no_series_annotations.append((engine, f"{engine} stops at {failed_scale}\n{annotation}"))
            continue

        scales, runtimes_ms = series_by_engine[engine]
        ax.plot(
            scales,
            runtimes_ms,
            marker="o",
            linewidth=2.5,
            markersize=6,
            label=engine,
            color=colours.get(engine),
        )

        if annotation_target is not None:
            failed_scale, annotation = annotation_target
            prior_scales = [scale for scale in scales if scale < failed_scale]
            prior_runtimes = [runtime for scale, runtime in zip(scales, runtimes_ms) if scale < failed_scale]
            if prior_scales and prior_runtimes:
                if len(annotation) > 72:
                    annotation = annotation[:69] + "..."
                ax.annotate(
                    f"{engine} stops at {failed_scale}\n{annotation}",
                    xy=(prior_scales[-1], prior_runtimes[-1]),
                    xytext=(12, 16),
                    textcoords="offset points",
                    fontsize=8.5,
                    color=colours.get(engine),
                    bbox={"boxstyle": "round,pad=0.3", "fc": "#fffdf8", "ec": colours.get(engine), "alpha": 0.95},
                    arrowprops={"arrowstyle": "->", "color": colours.get(engine), "lw": 1.2},
                )

    for index, (engine, text) in enumerate(no_series_annotations):
        ax.text(
            0.02,
            0.98 - index * 0.12,
            text,
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=8.5,
            color=colours.get(engine),
            bbox={"boxstyle": "round,pad=0.3", "fc": "#fffdf8", "ec": colours.get(engine), "alpha": 0.95},
        )

    ax.set_xlim(left=min_scale)
    major_tick_start = min_scale if min_scale % 2 == 0 else min_scale + 1
    ax.set_xticks(list(range(major_tick_start, max_scale + 1, 2)))
    ax.xaxis.set_minor_locator(MultipleLocator(1))
    ax.set_xlabel("Orders Scale")
    ax.set_ylabel("Best Runtime (ms)")
    ax.set_title("Join Scale Runtime by Engine")
    ax.legend(title="Engine")
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.grid(True, axis="x", which="major", linestyle=":", alpha=0.20)
    ax.grid(True, axis="x", which="minor", linestyle=":", alpha=0.08)

    fig.savefig(PLOT_PATH, format="webp", dpi=180)
    plt.close(fig)
    return PLOT_PATH


def print_report(rows_by_engine: dict[str, dict[str, dict[str, str]]], query_paths: dict[str, str], theorems: list[str]) -> None:
    headers = ["Theorem", "Query"]
    separators = ["---", "---"]
    for engine in ENGINE_COLUMNS:
        headers.extend([f"{engine} Runtime (ms)", f"{engine} StdDev (ms)"])
        separators.extend(["---:", "---:"])

    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(separators) + " |")

    for theorem in theorems:
        row_values = [theorem, query_paths.get(theorem, "-")]
        for engine in ENGINE_COLUMNS:
            runtime = rows_by_engine.get(engine, {}).get(theorem)
            if has_complete_ok_runtime(runtime):
                best_ms = int(runtime["RuntimeBest"]) / 1000.0
                stddev_ms = float(runtime["RuntimeStdDev"]) / 1000.0
                row_values.extend([f"{best_ms:.2f}", f"{stddev_ms:.2f}"])
            elif runtime and runtime.get("RunStatus") in {"ERROR", "TIMEOUT"}:
                row_values.extend([runtime.get("RunStatus", "ERROR"), runtime.get("ErrorMessage", "-").splitlines()[0]])
            else:
                row_values.extend(["-", "-"])
        print("| " + " | ".join(row_values) + " |")


def render_report(scales: list[int]) -> int:
    theorems = theorem_names(scales)
    rows_by_engine, sql_by_theorem = collect_rows(theorems)
    if not rows_by_engine:
        print("No EE-JOIN-SCALE rows found under proof/.", file=sys.stderr)
        return 1

    query_paths = write_query_files(sql_by_theorem)

    print_report(rows_by_engine, query_paths, theorems)
    plot_path = render_plot(rows_by_engine, theorems)
    if plot_path is not None:
        print(f"\nPlot written to: {plot_path}")
    return 0


def compose_cmd(*args: str) -> list[str]:
    return ["docker", "compose", "-f", str(DOCKER_COMPOSE_FILE), *args]


def kill_latent_containers() -> None:
    subprocess.run(
        compose_cmd("down", "--remove-orphans"),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    docker_cleanup_targets = (
        "dbprove-datafusion:latest",
        "dbprove-duckdb-bench:latest",
    )

    for image in docker_cleanup_targets:
        ids = subprocess.run(
            ["docker", "ps", "-aq", "--filter", f"ancestor={image}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if ids.returncode != 0:
            continue

        container_ids = [line.strip() for line in ids.stdout.splitlines() if line.strip()]
        if not container_ids:
            continue

        subprocess.run(
            ["docker", "rm", "-f", *container_ids],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )


def start_container_service(engine: dict[str, str | bool]) -> int:
    if engine["arg"] == "duckdb":
        artifact_result = subprocess.run(
            [str(DUCKDB_ARTIFACT_SCRIPT)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=WARMUP_TIMEOUT_SECONDS,
        )
        if artifact_result.returncode != 0:
            print("Failed to prepare Ubuntu DuckDB dbprove artifact.", file=sys.stderr)
            if artifact_result.stdout:
                print("\nstdout tail:\n", file=sys.stderr)
                print(artifact_result.stdout[-4000:], file=sys.stderr)
            if artifact_result.stderr:
                print("\nstderr tail:\n", file=sys.stderr)
                print(artifact_result.stderr[-4000:], file=sys.stderr)
            return artifact_result.returncode
        return 0

    build_service = engine.get("compose_build_service")
    if isinstance(build_service, str):
        result = subprocess.run(
            compose_cmd("build", build_service),
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=WARMUP_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            print(f"Failed to build container service for {engine['name']}.", file=sys.stderr)
            if result.stdout:
                print("\nstdout tail:\n", file=sys.stderr)
                print(result.stdout[-4000:], file=sys.stderr)
            if result.stderr:
                print("\nstderr tail:\n", file=sys.stderr)
                print(result.stderr[-4000:], file=sys.stderr)
            return result.returncode

    service = engine.get("compose_service")
    if not isinstance(service, str):
        return 0

    result = subprocess.run(
        compose_cmd("up", "-d", service),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=WARMUP_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        print(f"Failed to start container service for {engine['name']}.", file=sys.stderr)
        if result.stdout:
            print("\nstdout tail:\n", file=sys.stderr)
            print(result.stdout[-4000:], file=sys.stderr)
        if result.stderr:
            print("\nstderr tail:\n", file=sys.stderr)
            print(result.stderr[-4000:], file=sys.stderr)
    return result.returncode


def wait_for_engine_ready(engine: dict[str, str | bool]) -> int:
    if engine["arg"] != "trino":
        return 0

    deadline = time.time() + 120
    last_error = "unknown error"
    while time.time() < deadline:
        try:
            with urlopen("http://localhost:8080/v1/info", timeout=2) as response:
                if response.status == 200:
                    return 0
                last_error = f"http {response.status}"
        except URLError as exc:
            last_error = str(exc.reason)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)

    print(f"{engine['name']} readiness check failed: {last_error}", file=sys.stderr)
    return 1


def stop_container_service(engine: dict[str, str | bool]) -> None:
    service = engine.get("compose_service")
    if not isinstance(service, str):
        return
    subprocess.run(
        compose_cmd("stop", service),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


def warm_engine(engine: dict[str, str | bool]) -> int:
    if not engine["warmup"]:
        return 0
    warmup_cmd = dbprove_command(engine, ["-e", str(engine["arg"]), "-T", "TPCH-Q01"])
    print(f"Warming {engine['name']} TPCH dataset before timed join runs...", file=sys.stderr)

    attempts = 1 if engine["arg"] != "trino" else 6
    for attempt in range(1, attempts + 1):
        try:
            warmup = subprocess.run(warmup_cmd, cwd=ROOT, capture_output=True, text=True, timeout=WARMUP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            print(f"{engine['name']} warmup exceeded {WARMUP_TIMEOUT_SECONDS}s.", file=sys.stderr)
            return 124

        if warmup.returncode == 0:
            return 0

        trino_initializing = (
            engine["arg"] == "trino"
            and "Trino server is still initializing" in warmup.stdout
        )
        if trino_initializing and attempt < attempts:
            time.sleep(5)
            continue

        print(f"{engine['name']} warmup failed.", file=sys.stderr)
        if warmup.stdout:
            print("\nstdout tail:\n", file=sys.stderr)
            print(warmup.stdout[-4000:], file=sys.stderr)
        if warmup.stderr:
            print("\nstderr tail:\n", file=sys.stderr)
            print(warmup.stderr[-4000:], file=sys.stderr)
        return warmup.returncode

    return 1


def run_engine(engine: dict[str, str | bool], scales: list[int]) -> int:
    try:
        rc = start_container_service(engine)
        if rc != 0:
            return rc

        rc = wait_for_engine_ready(engine)
        if rc != 0:
            return rc

        rc = warm_engine(engine)
        if rc != 0:
            return rc

        requested_theorems = theorem_names(scales)
        cmd = dbprove_command(engine, [
            "-e",
            str(engine["arg"]),
            "-T",
            ",".join(requested_theorems),
            "--query-timeout",
            str(QUERY_TIMEOUT_SECONDS),
            "--timing-runs",
            str(TIMING_RUNS),
        ])

        start = time.time()
        try:
            proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=ENGINE_RUN_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            elapsed = time.time() - start
            print(f"\n## {engine['name']}")
            print("| Theorem | Status | Wall (s) | Best (ms) | StdDev (ms) | Runs |")
            print("| --- | --- | ---: | ---: | ---: | ---: |")
            print(f"| ALL | TIMEOUT | {elapsed:.2f} | - | - | - |")
            return 124

        proof_csv = latest_proof_csv(str(engine["name"]))
        rows = parse_proof_rows(proof_csv, requested_theorems) if proof_csv else {}

        print(f"\n## {engine['name']}")
        print("| Theorem | Status | Wall (s) | Best (ms) | StdDev (ms) | Runs |")
        print("| --- | --- | ---: | ---: | ---: | ---: |")

        for theorem in requested_theorems:
            row = rows.get(theorem)
            if row is None:
                print(f"| {theorem} | NOT_RUN | - | - | - | - |")
                continue

            status = row.get("RunStatus", "UNKNOWN")
            if status == "OK" and {"RuntimeBest", "RuntimeStdDev", "RuntimeRuns"} <= row.keys():
                best_ms = int(row["RuntimeBest"]) / 1000.0
                stddev_ms = float(row["RuntimeStdDev"]) / 1000.0
                print(f"| {theorem} | OK | - | {best_ms:.2f} | {stddev_ms:.2f} | {row['RuntimeRuns']} |")
            else:
                print(f"| {theorem} | {status} | - | - | - | - |")

        if proc.returncode != 0 and proc.stderr:
            print("\nstderr tail:\n")
            print(proc.stderr[-4000:])

        return proc.returncode
    finally:
        stop_container_service(engine)


def dbprove_command(engine: dict[str, str | bool], args: list[str]) -> list[str]:
    if engine["arg"] == "duckdb":
        args = [*args, "--parquet-dir", "/mnt/tpch-tmpfs"]
        command = shlex.join(["/opt/dbprove/bin/dbprove", *args])
        return [
            "docker",
            "run",
            "--rm",
            "--tmpfs",
            "/mnt/tpch-tmpfs:size=1g",
            DUCKDB_IMAGE,
            "bash",
            "-lc",
            command,
        ]
    return [str(DBPROVE), *args]


def main() -> int:
    if not DBPROVE.exists():
        print(f"dbprove binary not found: {DBPROVE}", file=sys.stderr)
        return 1

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("engines", nargs="*")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--query-timeout", type=int, default=30)
    parser.add_argument("--timing-runs", type=int, default=3)
    parser.add_argument("--max-scale", type=int, default=max(SCALES))
    args, _ = parser.parse_known_args(sys.argv[1:])

    global QUERY_TIMEOUT_SECONDS
    QUERY_TIMEOUT_SECONDS = args.query_timeout

    global TIMING_RUNS
    TIMING_RUNS = args.timing_runs

    global ENGINE_RUN_TIMEOUT_SECONDS
    scales = [scale for scale in SCALES if scale <= args.max_scale]
    if not scales:
        print("No scales selected.", file=sys.stderr)
        return 1
    ENGINE_RUN_TIMEOUT_SECONDS = max(QUERY_TIMEOUT_SECONDS * len(scales) * 12, 600)

    if args.report_only:
        return render_report(scales)

    kill_latent_containers()

    requested_engines = set(args.engines)
    engines = [engine for engine in ENGINES if not requested_engines or engine["arg"] in requested_engines]
    if requested_engines and not engines:
        print(f"No matching engines for: {', '.join(sorted(requested_engines))}", file=sys.stderr)
        return 1

    exit_code = 0
    for engine in engines:
        rc = run_engine(engine, scales)
        if rc != 0:
            exit_code = rc

    report_rc = render_report(scales)
    return exit_code or report_rc


if __name__ == "__main__":
    raise SystemExit(main())
