#!/usr/bin/env python3
import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


TRINO_SERVER_URL = os.environ.get("TRINO_SERVER_URL", "http://127.0.0.1:8080")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "tpch")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "default")
TRINO_USER = os.environ.get("TRINO_USER", "dbprove")
WRAPPER_PORT = int(os.environ.get("ICEBERG_WRAPPER_PORT", "19130"))
TABLE_DATA_MARKER = os.environ.get("ICEBERG_TABLE_DATA_MARKER", "table_data")


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _run_trino_sql(statement: str) -> str:
    args = [
        "/usr/bin/trino",
        "--server",
        TRINO_SERVER_URL,
        "--catalog",
        TRINO_CATALOG,
        "--schema",
        TRINO_SCHEMA,
        "--user",
        TRINO_USER,
        "--execute",
        statement,
    ]
    env = dict(os.environ)
    env["JAVA_HOME"] = env.get("TRINO_JAVA_HOME", "/opt/trino-java")
    result = subprocess.run(args, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"trino exited with {result.returncode}")
    return result.stdout


def _wait_for_trino() -> None:
    deadline = time.time() + 180
    last_error = "trino did not become ready"
    while time.time() < deadline:
        try:
            _run_trino_sql("SELECT 1")
            return
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(1)
    raise RuntimeError(f"Timed out waiting for Trino readiness: {last_error}")


def _render_type(column: dict) -> str:
    kind = column["kind"].upper()
    if kind == "SMALLINT":
        return "SMALLINT"
    if kind == "INT":
        return "INTEGER"
    if kind == "BIGINT":
        return "BIGINT"
    if kind == "REAL":
        return "REAL"
    if kind == "DOUBLE":
        return "DOUBLE"
    if kind == "DECIMAL":
        precision = column.get("decimal_precision")
        scale = column.get("decimal_scale")
        if precision is None:
            return "DECIMAL"
        if scale is None:
            return f"DECIMAL({precision})"
        return f"DECIMAL({precision}, {scale})"
    if kind == "STRING":
        length = column.get("string_length")
        if length is None:
            return "VARCHAR"
        return f"VARCHAR({length})"
    if kind == "DATE":
        return "DATE"
    if kind == "TIME":
        return "TIME"
    if kind == "DATETIME":
        return "TIMESTAMP"
    raise RuntimeError(f"Unsupported SQL type kind for Iceberg registration: {kind}")


def _table_uri_for_stem(source_stem: str) -> str:
    parts = []
    seen_table_data = False
    for part in source_stem.split("/"):
        if not part or part == ".":
            continue
        if not seen_table_data:
            if part == TABLE_DATA_MARKER:
                seen_table_data = True
            continue
        parts.append(part)

    if not parts:
        parts.append(source_stem.rsplit("/", 1)[-1])

    return "local:///table_data/" + "/".join(parts) + ".parquet"


def _register_table(payload: dict) -> dict:
    table_name = payload["table_name"]
    columns = payload["columns"]
    source_stems = payload["source_stems"]

    if not columns:
        raise RuntimeError("register-table requires at least one column")
    if not source_stems:
        raise RuntimeError("register-table requires at least one source stem")

    split = table_name.split(".", 1)
    if len(split) == 2:
        schema_name, local_table = split
        fq_schema = f"{TRINO_CATALOG}.{schema_name}"
        fq_table = f"{TRINO_CATALOG}.{schema_name}.{local_table}"
        create_schema = (
            f"CREATE SCHEMA IF NOT EXISTS {fq_schema} "
            f"WITH (location = 'local:///warehouse/{schema_name}')"
        )
        _run_trino_sql(create_schema)
    else:
        fq_table = f"{TRINO_CATALOG}.{table_name}"

    _run_trino_sql(f"DROP TABLE IF EXISTS {fq_table}")

    rendered_columns = []
    for column in columns:
        rendered_column = f"{column['name']} {_render_type(column)}"
        if not column.get("is_null", True):
            rendered_column += " NOT NULL"
        rendered_columns.append(rendered_column)

    create_table = "CREATE TABLE " + fq_table + " (\n  " + ",\n  ".join(rendered_columns) + "\n)\nWITH (\n  format = 'PARQUET'\n)"
    _run_trino_sql(create_table)

    for source_stem in source_stems:
        add_files = (
            f"ALTER TABLE {fq_table} EXECUTE add_files(\n"
            f"  location => '{_table_uri_for_stem(source_stem)}',\n"
            f"  format => 'PARQUET'\n"
            f")"
        )
        _run_trino_sql(add_files)

    return {"status": "ok", "table_name": table_name, "source_file_count": len(source_stems)}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            try:
                _wait_for_trino()
                _json_response(self, 200, {"status": "ok"})
            except Exception as exc:  # noqa: BLE001
                _json_response(self, 503, {"status": "error", "error": str(exc)})
            return

        _json_response(self, 404, {"status": "error", "error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/register-table":
            _json_response(self, 404, {"status": "error", "error": "not found"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(content_length))
            _wait_for_trino()
            result = _register_table(payload)
            _json_response(self, 200, result)
        except Exception as exc:  # noqa: BLE001
            _json_response(self, 500, {"status": "error", "error": str(exc)})

    def log_message(self, format: str, *args) -> None:
        print(f"iceberg-wrapper: {format % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", WRAPPER_PORT), Handler)
    print(f"iceberg-wrapper: listening on 0.0.0.0:{WRAPPER_PORT}", flush=True)
    server.serve_forever()
