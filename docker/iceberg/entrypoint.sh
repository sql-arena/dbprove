#!/usr/bin/env bash
set -euo pipefail

mkdir -p /data/trino/spill /opt/dbprove/warehouse

python3 /opt/dbprove/iceberg_wrapper.py &
JAVA_HOME=/opt/trino-java /usr/lib/trino/bin/run-trino &

exec /opt/jboss/container/java/run/run-java.sh
