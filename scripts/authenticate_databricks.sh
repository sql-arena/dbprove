#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/env.sh" ]; then
    # shellcheck disable=SC1091
    . "$PROJECT_ROOT/env.sh"
fi

WORKSPACE_URL="${DATABRICKS_ENDPOINT:-}"

if [ -z "$WORKSPACE_URL" ] && [ -n "${DATABRICKS_HOST:-}" ]; then
    WORKSPACE_URL="https://${DATABRICKS_HOST%/}"
fi

if [ -z "$WORKSPACE_URL" ] && [ -n "${DATABRICKS_HOSTNAME:-}" ]; then
    WORKSPACE_URL="https://${DATABRICKS_HOSTNAME%/}"
fi

if [ -z "$WORKSPACE_URL" ]; then
    echo "Error: set DATABRICKS_ENDPOINT, DATABRICKS_HOST, or DATABRICKS_HOSTNAME before authenticating." >&2
    exit 1
fi

if [[ "$WORKSPACE_URL" != https://* ]]; then
    WORKSPACE_URL="https://${WORKSPACE_URL#http://}"
fi

cd "$PROJECT_ROOT"
node scripts/dump_databricks_plan.mjs \
    --workspace "$WORKSPACE_URL" \
    --headless 0 \
    -v

echo "Browser session closed. If login completed, the Playwright profile is ready for dbprove."
