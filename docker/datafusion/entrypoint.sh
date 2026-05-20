#!/usr/bin/env bash
set -euo pipefail

mkdir -p /mnt/tpch-tmpfs
mkdir -p /mnt/tpch-tmpfs/join_scale
cp -R /opt/join-scale-source/. /mnt/tpch-tmpfs/join_scale/

exec datafusion-cli --data-path /workspace -m 2g -f /opt/datafusion/bootstrap.sql "$@"
