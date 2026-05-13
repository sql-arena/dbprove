#!/usr/bin/env bash
set -euo pipefail

exec datafusion-cli --data-path /workspace -f /opt/datafusion/bootstrap.sql "$@"
