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
PLOT_PATH = PROOF_ROOT / "join_scale_runtime.webp"
SORT_PLOT_PATH = PROOF_ROOT / "sort_scale_runtime.webp"
AGG_PLOT_PATH = PROOF_ROOT / "agg_scale_runtime.webp"
PARQUET_SOURCE_DIR = ROOT / "docker" / "datafusion" / "tpch" / "sf1"
JOIN_SCALE_PARQUET_DIR = ROOT / "run" / "materialized" / "join_scale"


def default_dbprove_path() -> Path:
    env_path = os.environ.get("DBPROVE_BIN")
    if env_path:
        return Path(env_path)

    release = ROOT / "out/build/osx-arm-release/src/dbprove/dbprove"
    if release.exists():
        return release

    return ROOT / "out/build/osx-arm-base/src/dbprove/dbprove"


DBPROVE = default_dbprove_path()
SCALES = [1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20]
QUERY_TIMEOUT_SECONDS = 60
TIMING_RUNS = 1
WARMUP_TIMEOUT_SECONDS = 1800
ENGINE_RUN_TIMEOUT_SECONDS = max(QUERY_TIMEOUT_SECONDS * len(SCALES) * 12, 600)
ENGINE_COLUMNS = ["DuckDB", "DataFusion", "Trino"]
DOCKER_COMPOSE_FILE = ROOT / "docker" / "docker-compose.yml"
DUCKDB_IMAGE = "dbprove-duckdb-bench:latest"
COMPOSE_PROJECT_NAME = "dbprove-scale"
LEGACY_COMPOSE_CONTAINER_NAMES = ("docker-nessie-1", "docker-trino-1", "docker-datafusion-1")
FORCE_DUCKDB_BUILD = os.environ.get("DBPROVE_FORCE_DUCKDB_BUILD", "").lower() in {"1", "true", "yes"}

ENGINES = [
    {"arg": "duckdb", "name": "DuckDB", "warmup": False},
    {"arg": "datafusion", "name": "DataFusion", "warmup": False, "compose_build_service": "datafusion", "compose_service": "datafusion"},
    {"arg": "trino", "name": "Trino", "warmup": True, "compose_build_service": "trino", "compose_service": "trino"},
]


def prepare_join_scale_artifacts() -> int:
    cmd = [
        str(DBPROVE),
        "--prepare-ee-join-scale",
        "--parquet-dir",
        str(PARQUET_SOURCE_DIR),
    ]
    print("Preparing EE join-scale parquet artifacts on host...", file=sys.stderr)
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=WARMUP_TIMEOUT_SECONDS,
    )
    if proc.stdout:
        print(proc.stdout, file=sys.stderr, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.returncode != 0:
        print("Failed to prepare EE join-scale parquet artifacts.", file=sys.stderr)
        if proc.stderr:
            print(proc.stderr[-4000:], file=sys.stderr)
    return proc.returncode


def theorem_names(scales: list[int]) -> list[str]:
    return [f"EE-JOIN-SCALE-{scale:02d}" for scale in scales]


def sort_theorem_names(scales: list[int]) -> list[str]:
    return [f"EE-SORT-SCALE-{scale:02d}" for scale in scales]


def agg_theorem_names(scales: list[int]) -> list[str]:
    return [f"EE-AGG-SCALE-{scale:02d}" for scale in scales]


def requested_theorem_names(scales: list[int], suite: str) -> list[str]:
    if suite in {"all", "both"}:
        return theorem_names(scales) + sort_theorem_names(scales) + agg_theorem_names(scales)
    if suite == "sort":
        return sort_theorem_names(scales)
    if suite == "agg":
        return agg_theorem_names(scales)
    return theorem_names(scales)


def suite_configs(scales: list[int]) -> list[tuple[str, list[str], Path]]:
    return [
        ("Join Scale Runtime by Engine", discover_scale_theorems("JOIN"), PLOT_PATH),
        ("Sort Scale Runtime by Engine", discover_scale_theorems("SORT"), SORT_PLOT_PATH),
        ("Aggregation Scale Runtime by Engine", discover_scale_theorems("AGG"), AGG_PLOT_PATH),
    ]


def scale_key(theorem_name: str) -> int:
    return int(theorem_name.rsplit("-", 1)[-1])


def latest_proof_dir(engine_name: str) -> Path | None:
    proof_root = PROOF_ROOT / engine_name
    if not proof_root.exists():
        return None
    candidates = [path for path in proof_root.iterdir() if path.is_dir()]
    candidates.sort(key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def theorem_proof_csv(engine_name: str, theorem: str) -> Path | None:
    proof_dir = latest_proof_dir(engine_name)
    if proof_dir is None:
        return None
    proof_csv = proof_dir / f"{theorem}.csv"
    return proof_csv if proof_csv.exists() else None


def parse_proof_rows(engine_name: str, theorems: list[str]) -> dict[str, dict[str, str]]:
    rows_by_theorem: dict[str, dict[str, str]] = {}
    for theorem in theorems:
        proof_csv = theorem_proof_csv(engine_name, theorem)
        if proof_csv is None:
            continue
        with proof_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="|", quotechar='"')
            for row in reader:
                if row["THEOREM"] != theorem:
                    continue
                rows_by_theorem.setdefault(theorem, {})[row["PROOF_NAME"]] = row["PROOF_VALUE"]
    return rows_by_theorem


def remove_theorem_proof_files(engine_name: str, theorems: list[str]) -> None:
    proof_dir = latest_proof_dir(engine_name)
    if proof_dir is None:
        return
    for theorem in theorems:
        proof_csv = proof_dir / f"{theorem}.csv"
        if proof_csv.exists():
            proof_csv.unlink()


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


def collect_rows(theorems: list[str]) -> tuple[dict[str, dict[str, dict[str, str]]], dict[str, set[str]]]:
    rows_by_engine: dict[str, dict[str, dict[str, str]]] = {}
    tags_by_theorem: dict[str, set[str]] = {}
    for proof_csv in sorted(PROOF_ROOT.glob("*/*/*.csv"), key=lambda path: path.stat().st_mtime):
        if proof_csv.name.endswith("_proof.csv"):
            continue
        with proof_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="|", quotechar='"')
            for row in reader:
                theorem = row["THEOREM"]
                if theorem not in theorems:
                    continue
                engine = row["ENGINE"]
                rows_by_engine.setdefault(engine, {}).setdefault(theorem, {})[row["PROOF_NAME"]] = row["PROOF_VALUE"]
                tags_by_theorem.setdefault(
                    theorem,
                    {tag.strip() for tag in row.get("TAGS", "").split(",") if tag.strip()},
                )
    return rows_by_engine, tags_by_theorem


def discover_scale_theorems(suite_tag: str) -> list[str]:
    discovered: dict[str, set[str]] = {}
    for proof_csv in sorted(PROOF_ROOT.glob("*/*/*.csv"), key=lambda path: path.stat().st_mtime):
        if proof_csv.name.endswith("_proof.csv"):
            continue
        with proof_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="|", quotechar='"')
            for row in reader:
                theorem = row["THEOREM"]
                tags = {tag.strip() for tag in row.get("TAGS", "").split(",") if tag.strip()}
                if "scale" not in tags or suite_tag not in tags:
                    continue
                discovered.setdefault(theorem, tags)

    return sorted(discovered.keys(), key=scale_key)


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


def render_plot(rows_by_engine: dict[str, dict[str, dict[str, str]]], theorems: list[str], plot_path: Path, title: str) -> Path | None:
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

    visible_min_runtime = min(
        runtime
        for _, runtimes in series_by_engine.values()
        for runtime in runtimes
    )

    colours = {
        "DuckDB": "#d18a00",
        "DataFusion": "#2f2f2f",
        "Trino": "#7e57c2",
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
                failure_y = max(visible_min_runtime * 0.85, 1.0)
                ax.scatter(
                    [failed_scale],
                    [failure_y],
                    marker="x",
                    s=90,
                    linewidths=2.2,
                    color=colours.get(engine),
                    label=engine,
                    zorder=5,
                )
                ax.annotate(
                    f"{engine} stops at {failed_scale}\n{annotation}",
                    xy=(failed_scale, failure_y),
                    xytext=(10, 10),
                    textcoords="offset points",
                    fontsize=8.5,
                    color=colours.get(engine),
                    bbox={"boxstyle": "round,pad=0.3", "fc": "#fffdf8", "ec": colours.get(engine), "alpha": 0.95},
                    arrowprops={"arrowstyle": "->", "color": colours.get(engine), "lw": 1.2},
                )
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
            0.62,
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
    ax.set_title(title)
    ax.legend(title="Engine")
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.grid(True, axis="x", which="major", linestyle=":", alpha=0.20)
    ax.grid(True, axis="x", which="minor", linestyle=":", alpha=0.08)

    fig.savefig(plot_path, format="webp", dpi=180)
    plt.close(fig)
    return plot_path


def print_report(rows_by_engine: dict[str, dict[str, dict[str, str]]], theorems: list[str]) -> None:
    headers = ["Theorem"]
    separators = ["---"]
    for engine in ENGINE_COLUMNS:
        headers.extend([f"{engine} Runtime (ms)", f"{engine} StdDev (ms)"])
        separators.extend(["---:", "---:"])

    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(separators) + " |")

    for theorem in theorems:
        row_values = [theorem]
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


def render_suite_report(theorems: list[str], plot_path: Path, title: str) -> int:
    rows_by_engine, _ = collect_rows(theorems)
    if not rows_by_engine:
        print(f"No {title} rows found under proof/.", file=sys.stderr)
        return 1

    print_report(rows_by_engine, theorems)
    rendered_plot = render_plot(rows_by_engine, theorems, plot_path, title)
    if rendered_plot is not None:
        print(f"\nPlot written to: {rendered_plot}")
    return 0


def render_report(scales: list[int]) -> int:
    reports = suite_configs(scales)
    exit_code = 0
    rendered_any = False
    for index, (title, theorems, plot_path) in enumerate(reports):
        rows_by_engine, _ = collect_rows(theorems)
        if not rows_by_engine:
            continue
        if rendered_any:
            print()
        print(f"## {title}")
        rc = render_suite_report(theorems, plot_path, title)
        rendered_any = True
        if rc != 0 and exit_code == 0:
            exit_code = rc

    if not rendered_any:
        print("No EE scale rows found under proof/.", file=sys.stderr)
        return 1
    return exit_code


def compose_cmd(*args: str) -> list[str]:
    return ["docker", "compose", "-p", COMPOSE_PROJECT_NAME, "-f", str(DOCKER_COMPOSE_FILE), *args]


def docker_image_exists(image: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.returncode == 0


def kill_latent_containers() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(DOCKER_COMPOSE_FILE), "down", "--remove-orphans"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
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

    subprocess.run(
        ["docker", "rm", "-f", *LEGACY_COMPOSE_CONTAINER_NAMES],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


def cleanup_compose_runtime_artifacts() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(DOCKER_COMPOSE_FILE), "down", "--remove-orphans"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    subprocess.run(
        compose_cmd("down", "--remove-orphans"),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    subprocess.run(
        ["docker", "rm", "-f", *LEGACY_COMPOSE_CONTAINER_NAMES],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        compose_ps = subprocess.run(
            compose_cmd("ps", "-a", "-q"),
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        legacy_ids: list[str] = []
        for name in LEGACY_COMPOSE_CONTAINER_NAMES:
            containers = subprocess.run(
                ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=30,
            )
            legacy_ids.extend(line.strip() for line in containers.stdout.splitlines() if line.strip())
        compose_ids = [line.strip() for line in compose_ps.stdout.splitlines() if line.strip()]
        if not compose_ids and not legacy_ids:
            return
        time.sleep(1)


def start_container_service(engine: dict[str, str | bool]) -> int:
    if engine["arg"] == "duckdb":
        if FORCE_DUCKDB_BUILD or not docker_image_exists(DUCKDB_IMAGE):
            build_result = subprocess.run(
                [
                    "docker",
                    "build",
                    "--network=host",
                    "--progress=plain",
                    "-f",
                    str(ROOT / "docker" / "duckdb" / "Dockerfile"),
                    "-t",
                    DUCKDB_IMAGE,
                    str(ROOT),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=WARMUP_TIMEOUT_SECONDS,
            )
            if build_result.returncode != 0:
                print("Failed to build DuckDB benchmark image.", file=sys.stderr)
                if build_result.stdout:
                    print("\nstdout tail:\n", file=sys.stderr)
                    print(build_result.stdout[-4000:], file=sys.stderr)
                if build_result.stderr:
                    print("\nstderr tail:\n", file=sys.stderr)
                    print(build_result.stderr[-4000:], file=sys.stderr)
                return build_result.returncode
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

    cleanup_compose_runtime_artifacts()

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
    if engine["arg"] == "datafusion":
        marker_cmd = compose_cmd("exec", "-T", "datafusion", "sh", "-lc", "test -f /tmp/datafusion-bootstrap-ready")
        deadline = time.time() + 300
        last_error = "bootstrap marker not present yet"
        while time.time() < deadline:
            marker = subprocess.run(
                marker_cmd,
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if marker.returncode == 0:
                return 0
            if marker.stderr.strip():
                last_error = marker.stderr.strip().splitlines()[-1]
            time.sleep(2)
        print(f"{engine['name']} readiness check failed: {last_error}", file=sys.stderr)
        return 1

    if engine["arg"] != "trino":
        return 0

    deadline = time.time() + 120
    last_error = "unknown error"
    while time.time() < deadline:
        try:
            with urlopen("http://localhost:8080/v1/info", timeout=2) as response:
                if response.status == 200:
                    marker = subprocess.run(
                        compose_cmd("exec", "-T", "trino", "sh", "-lc", "test -f /tmp/trino-bootstrap-ready"),
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if marker.returncode == 0:
                        return 0
                    last_error = "bootstrap marker not present yet"
                else:
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
    cleanup_compose_runtime_artifacts()


def warm_engine(engine: dict[str, str | bool], scales: list[int]) -> int:
    if not engine["warmup"]:
        return 0
    print(f"Warming {engine['name']} before timed join runs...", file=sys.stderr)

    if engine["arg"] == "trino":
        warmup_cmd = compose_cmd(
            "exec",
            "-T",
            "trino",
            "/usr/bin/trino",
            "--server",
            "http://127.0.0.1:8080",
            "--catalog",
            "tpch",
            "--schema",
            "sf1",
            "--execute",
            "SELECT 1 FROM tpch.sf1.lineitem_25x LIMIT 1",
        )
        attempts = 6
        for attempt in range(1, attempts + 1):
            try:
                warmup = subprocess.run(warmup_cmd, cwd=ROOT, capture_output=True, text=True, timeout=WARMUP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                print(f"{engine['name']} warmup exceeded {WARMUP_TIMEOUT_SECONDS}s.", file=sys.stderr)
                return 124
            if warmup.returncode == 0:
                return 0
            if attempt < attempts:
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

    warmup_cmd = dbprove_command(engine, ["-e", str(engine["arg"]), "-T", "TPCH-Q01"])
    try:
        warmup = subprocess.run(warmup_cmd, cwd=ROOT, capture_output=True, text=True, timeout=WARMUP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(f"{engine['name']} warmup exceeded {WARMUP_TIMEOUT_SECONDS}s.", file=sys.stderr)
        return 124

    if warmup.returncode == 0:
        return 0

    print(f"{engine['name']} warmup failed.", file=sys.stderr)
    if warmup.stdout:
        print("\nstdout tail:\n", file=sys.stderr)
        print(warmup.stdout[-4000:], file=sys.stderr)
    if warmup.stderr:
        print("\nstderr tail:\n", file=sys.stderr)
        print(warmup.stderr[-4000:], file=sys.stderr)
    return warmup.returncode


def run_engine(engine: dict[str, str | bool], scales: list[int], suite: str) -> int:
    if engine["arg"] == "trino":
        return run_trino_engine(engine, scales, suite)

    try:
        rc = start_container_service(engine)
        if rc != 0:
            return rc

        rc = wait_for_engine_ready(engine)
        if rc != 0:
            return rc

        rc = warm_engine(engine, scales)
        if rc != 0:
            return rc

        requested_theorems = requested_theorem_names(scales, suite)
        remove_theorem_proof_files(str(engine["name"]), requested_theorems)
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

        rows = parse_proof_rows(str(engine["name"]), requested_theorems)

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


def run_trino_engine(engine: dict[str, str | bool], scales: list[int], suite: str) -> int:
    requested_theorems = requested_theorem_names(scales, suite)
    remove_theorem_proof_files(str(engine["name"]), requested_theorems)
    exit_code = 0

    print(f"\n## {engine['name']}")
    print("| Theorem | Status | Wall (s) | Best (ms) | StdDev (ms) | Runs |")
    print("| --- | --- | ---: | ---: | ---: | ---: |")

    for theorem in requested_theorems:
        try:
            rc = start_container_service(engine)
            if rc != 0:
                exit_code = rc
                print(f"| {theorem} | START_FAILED | - | - | - | - |")
                continue

            rc = wait_for_engine_ready(engine)
            if rc != 0:
                exit_code = rc
                print(f"| {theorem} | NOT_READY | - | - | - | - |")
                continue

            rc = warm_engine(engine, scales)
            if rc != 0:
                exit_code = rc
                print(f"| {theorem} | WARMUP_FAILED | - | - | - | - |")
                continue

            cmd = dbprove_command(engine, [
                "-e",
                str(engine["arg"]),
                "-T",
                theorem,
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
                print(f"| {theorem} | TIMEOUT | {elapsed:.2f} | - | - | - |")
                exit_code = 124
                continue

            rows = parse_proof_rows(str(engine["name"]), [theorem])
            row = rows.get(theorem)
            if row is None:
                print(f"| {theorem} | NOT_RUN | - | - | - | - |")
                if proc.returncode != 0:
                    exit_code = proc.returncode
                continue

            status = row.get("RunStatus", "UNKNOWN")
            if status == "OK" and {"RuntimeBest", "RuntimeStdDev", "RuntimeRuns"} <= row.keys():
                best_ms = int(row["RuntimeBest"]) / 1000.0
                stddev_ms = float(row["RuntimeStdDev"]) / 1000.0
                print(f"| {theorem} | OK | - | {best_ms:.2f} | {stddev_ms:.2f} | {row['RuntimeRuns']} |")
            else:
                print(f"| {theorem} | {status} | - | - | - | - |")

            if proc.returncode != 0:
                exit_code = proc.returncode
                if proc.stderr:
                    print("\nstderr tail:\n")
                    print(proc.stderr[-4000:])
        finally:
            stop_container_service(engine)

    return exit_code


def dbprove_command(engine: dict[str, str | bool], args: list[str]) -> list[str]:
    if engine["arg"] == "duckdb":
        args = [*args, "--parquet-dir", "/mnt/tpch-tmpfs/join_scale"]
        command = shlex.join(["/opt/dbprove/bin/dbprove", *args])
        return [
            "docker",
            "run",
            "--rm",
            "--memory",
            "6g",
            "-v",
            f"{ROOT}:/repo",
            "-v",
            f"{JOIN_SCALE_PARQUET_DIR}:/opt/join-scale-source:ro",
            "--tmpfs",
            "/mnt/tpch-tmpfs:size=4g",
            DUCKDB_IMAGE,
            "bash",
            "-lc",
            command,
        ]
    return [str(DBPROVE), *args]


def parse_scales(selected: str | None, max_scale: int) -> list[int]:
    if selected:
        requested: list[int] = []
        for part in selected.split(","):
            part = part.strip()
            if not part:
                continue
            requested.append(int(part))
        scales = [scale for scale in SCALES if scale in requested and scale <= max_scale]
    else:
        scales = [scale for scale in SCALES if scale <= max_scale]
    return scales


def main() -> int:
    if not DBPROVE.exists():
        print(f"dbprove binary not found: {DBPROVE}", file=sys.stderr)
        return 1

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("engines", nargs="*")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--suite", choices=["join", "sort", "agg", "all", "both"], default="all")
    parser.add_argument("--query-timeout", type=int, default=60)
    parser.add_argument("--timing-runs", type=int, default=1)
    parser.add_argument("--max-scale", type=int, default=max(SCALES))
    parser.add_argument("--scales", help="Comma-separated subset of scales to run, for example 1,2,3,4,5")
    args, _ = parser.parse_known_args(sys.argv[1:])

    global QUERY_TIMEOUT_SECONDS
    QUERY_TIMEOUT_SECONDS = args.query_timeout

    global TIMING_RUNS
    TIMING_RUNS = args.timing_runs

    global ENGINE_RUN_TIMEOUT_SECONDS
    scales = parse_scales(args.scales, args.max_scale)
    if not scales:
        print("No scales selected.", file=sys.stderr)
        return 1
    ENGINE_RUN_TIMEOUT_SECONDS = max(QUERY_TIMEOUT_SECONDS * len(scales) * 12, 600)

    if args.report_only:
        if args.suite in {"all", "both"}:
            return render_report(scales)

        suite_index = {"join": 0, "sort": 1, "agg": 2}[args.suite]
        title, theorems, plot_path = suite_configs(scales)[suite_index]
        return render_suite_report(theorems, plot_path, title)

    kill_latent_containers()

    prepare_rc = prepare_join_scale_artifacts()
    if prepare_rc != 0:
        return prepare_rc

    requested_engines = set(args.engines)
    engines = [engine for engine in ENGINES if not requested_engines or engine["arg"] in requested_engines]
    if requested_engines and not engines:
        print(f"No matching engines for: {', '.join(sorted(requested_engines))}", file=sys.stderr)
        return 1

    exit_code = 0
    for engine in engines:
        rc = run_engine(engine, scales, args.suite)
        if rc != 0:
            exit_code = rc

    if args.suite in {"all", "both"}:
        report_rc = render_report(scales)
    else:
        suite_index = {"join": 0, "sort": 1, "agg": 2}[args.suite]
        title, theorems, plot_path = suite_configs(scales)[suite_index]
        report_rc = render_suite_report(theorems, plot_path, title)
    return exit_code or report_rc


if __name__ == "__main__":
    raise SystemExit(main())
