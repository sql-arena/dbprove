#!/usr/bin/env bash
set -euo pipefail

mkdir -p /mnt/tpch-tmpfs
cp /opt/tpch/sf1/orders.parquet /mnt/tpch-tmpfs/orders.parquet
cp /opt/tpch/sf1/lineitem.parquet /mnt/tpch-tmpfs/lineitem.parquet

exec "$@"
