#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${ROOT}/out/vcpkg_installed/arm64-osx"
INCLUDE_DIR="${BUILD_DIR}/include"
LIB_DIR="${BUILD_DIR}/lib"
SRC="${ROOT}/scripts/repros/duckdb_csv_repro.cpp"
CSV="${ROOT}/scripts/repros/duckdb_name_utf8_repro.csv"
BIN="/tmp/duckdb_csv_repro"

/usr/bin/clang++ -std=c++17 \
  -I"${INCLUDE_DIR}" \
  "${SRC}" \
  "${LIB_DIR}/libduckdb_static.a" \
  "${LIB_DIR}/libduckdb_generated_extension_loader.a" \
  "${LIB_DIR}/libcore_functions_extension.a" \
  "${LIB_DIR}/libparquet_extension.a" \
  -ldl -pthread \
  -o "${BIN}"

exec "${BIN}" "${CSV}"
