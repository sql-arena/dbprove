#!/bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"
RUN_DIR="$PROJECT_ROOT/run"
MOUNT_DIR="$RUN_DIR/mount"
BUILD_DIR="$PROJECT_ROOT/out/build/osx-arm-base"
DBPROVE_PATH="$BUILD_DIR/src/dbprove/dbprove"
GCS_TEST_URI="gs://sql-arena-data/tpc-h/"
GCS_BUCKET="sql-arena-data"
GCS_PREFIX="tpc-h/"
DATABRICKS_PLAN_CHECK_SCRIPT="$PROJECT_ROOT/scripts/dump_databricks_plan.mjs"
DATABRICKS_AUTH_SCRIPT="$PROJECT_ROOT/scripts/authenticate_databricks.sh"

SUPPORTED_ENGINES=(
    "postgresql"
    "clickhouse"
    "mssql"
    "duckdb"
    "databricks"
    "mariadb"
    "utopia"
)

VERBOSE_FLAG=""
RESET_FLAG=false
ENGINE_PARAM=""
GCS_ACCESS_VERIFIED=false
DATABRICKS_ACCESS_VERIFIED=false

usage() {
    echo "Usage: $0 [-v] [-r] -e <engine|ALL>"
    echo "  -v: Enable verbose output (passed to dbprove)"
    echo "  -r: Force restart of Docker-backed engines"
    echo "  -e: Engine to benchmark, or ALL to run every supported engine"
    echo "Supported dbprove engines: ${SUPPORTED_ENGINES[*]}"
    echo "Aliases: postgres/pg, clickhouse/ch, sqlserver, duck, mysql"
    echo "Note: Docker has a Trino service, but dbprove does not currently expose a Trino engine."
    exit 1
}

source_env_if_present() {
    if [ -f "$PROJECT_ROOT/env.sh" ]; then
        # shellcheck disable=SC1091
        . "$PROJECT_ROOT/env.sh"
    fi

    if [ -z "${DATABRICKS_HOST:-}" ]; then
        if [ -n "${DATABRICKS_ENDPOINT:-}" ]; then
            local normalized="${DATABRICKS_ENDPOINT#https://}"
            normalized="${normalized#http://}"
            normalized="${normalized%/}"
            export DATABRICKS_HOST="$normalized"
        elif [ -n "${DATABRICKS_HOSTNAME:-}" ]; then
            export DATABRICKS_HOST="$DATABRICKS_HOSTNAME"
        fi
    fi
}

normalize_engine() {
    local engine_input
    engine_input=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')

    case "$engine_input" in
        all)
            printf 'ALL\n'
            ;;
        postgresql|postgres|pg)
            printf 'postgresql\n'
            ;;
        clickhouse|ch)
            printf 'clickhouse\n'
            ;;
        mssql|sqlserver|"sql server"|azurefabricwarehouse)
            printf 'mssql\n'
            ;;
        duckdb|duck)
            printf 'duckdb\n'
            ;;
        databricks)
            printf 'databricks\n'
            ;;
        yellowbrick|yb|ybd)
            echo "Error: Yellowbrick is temporarily disabled in benchmark.sh." >&2
            exit 1
            ;;
        mariadb|mysql)
            printf 'mariadb\n'
            ;;
        utopia)
            printf 'utopia\n'
            ;;
        trino)
            echo "Error: Trino has a Docker service, but dbprove does not currently support '-e trino'." >&2
            exit 1
            ;;
        snowflake)
            echo "Error: Snowflake is scaffolded in the source tree, but benchmark.sh only runs engines currently exposed by dbprove." >&2
            exit 1
            ;;
        sqlite|sqlite3)
            echo "Error: SQLite is scaffolded in the source tree, but benchmark.sh does not support it yet." >&2
            exit 1
            ;;
        *)
            echo "Error: Engine '$1' not recognized." >&2
            usage
            ;;
    esac
}

engine_service_name() {
    case "$1" in
        postgresql|clickhouse|mssql)
            printf '%s\n' "$1"
            ;;
        *)
            printf '\n'
            ;;
    esac
}

engine_requires_gcs() {
    case "$1" in
        databricks)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

get_databricks_workspace() {
    if [ -n "${DATABRICKS_HOST:-}" ]; then
        printf 'https://%s\n' "${DATABRICKS_HOST%/}"
        return 0
    fi

    if [ -n "${DATABRICKS_ENDPOINT:-}" ]; then
        local endpoint="${DATABRICKS_ENDPOINT%/}"
        if [[ "$endpoint" != https://* ]]; then
            endpoint="https://${endpoint#http://}"
        fi
        printf '%s\n' "$endpoint"
        return 0
    fi

    if [ -n "${DATABRICKS_HOSTNAME:-}" ]; then
        printf 'https://%s\n' "${DATABRICKS_HOSTNAME%/}"
        return 0
    fi

    echo "Error: Databricks requires DATABRICKS_HOST, DATABRICKS_ENDPOINT, or DATABRICKS_HOSTNAME to be set." >&2
    exit 1
}

ensure_gcloud_access() {
    if [ "$GCS_ACCESS_VERIFIED" = true ]; then
        return
    fi

    echo "Checking application-default access to $GCS_TEST_URI..."

    if ! command -v gcloud >/dev/null 2>&1; then
        echo "Error: gcloud CLI is not installed. Install it, then rerun this script." >&2
        exit 1
    fi

    if ! command -v curl >/dev/null 2>&1; then
        echo "Error: curl is required for the GCS application-default auth check." >&2
        exit 1
    fi

    local access_token=""
    if ! access_token=$(gcloud auth application-default print-access-token 2>/dev/null); then
        echo "Application default credentials not available. Launching 'gcloud auth application-default login'..."
        gcloud auth application-default login

        if ! access_token=$(gcloud auth application-default print-access-token 2>/dev/null); then
            echo "Error: Unable to obtain an application-default access token after login." >&2
            exit 1
        fi
    fi

    local http_code
    http_code=$(curl -sS -o /dev/null -w "%{http_code}" \
        -H "Authorization: Bearer $access_token" \
        "https://storage.googleapis.com/storage/v1/b/${GCS_BUCKET}/o?maxResults=1&prefix=${GCS_PREFIX}" || true)

    case "$http_code" in
        200)
            GCS_ACCESS_VERIFIED=true
            echo "GCS application-default access verified."
            ;;
        401)
            echo "Error: An application-default token exists, but GCS rejected it with HTTP 401. Re-run 'gcloud auth application-default login' if the credentials are stale." >&2
            exit 1
            ;;
        403)
            echo "Error: Application-default credentials are present, but they do not have access to $GCS_TEST_URI." >&2
            exit 1
            ;;
        *)
            echo "Error: Unable to verify application-default access to $GCS_TEST_URI (HTTP $http_code)." >&2
            exit 1
            ;;
    esac
}

ensure_databricks_access() {
    if [ "$DATABRICKS_ACCESS_VERIFIED" = true ]; then
        return
    fi

    local workspace
    workspace=$(get_databricks_workspace)

    echo "Checking Databricks access for $workspace..."

    if ! command -v node >/dev/null 2>&1; then
        echo "Error: Node.js is required for Databricks authentication checks." >&2
        exit 1
    fi

    if [ ! -f "$DATABRICKS_PLAN_CHECK_SCRIPT" ]; then
        echo "Error: Databricks plan check script not found at $DATABRICKS_PLAN_CHECK_SCRIPT." >&2
        exit 1
    fi

    if node "$DATABRICKS_PLAN_CHECK_SCRIPT" --workspace "$workspace" --headless 1 >/dev/null 2>&1; then
        DATABRICKS_ACCESS_VERIFIED=true
        echo "Databricks access verified."
        return
    fi

    if [ ! -x "$DATABRICKS_AUTH_SCRIPT" ]; then
        echo "Error: Databricks auth helper not found or not executable at $DATABRICKS_AUTH_SCRIPT." >&2
        exit 1
    fi

    echo "Databricks session not ready. Launching $DATABRICKS_AUTH_SCRIPT..."
    "$DATABRICKS_AUTH_SCRIPT"

    if ! node "$DATABRICKS_PLAN_CHECK_SCRIPT" --workspace "$workspace" --headless 1 >/dev/null 2>&1; then
        echo "Error: Databricks is still not accessible after running the authentication helper." >&2
        exit 1
    fi

    DATABRICKS_ACCESS_VERIFIED=true
    echo "Databricks access verified."
}

ensure_docker_running() {
    echo "Checking if Docker daemon is running..."
    if ! docker info >/dev/null 2>&1; then
        echo "Docker daemon not running. Attempting to start Docker..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            open --background -a Docker
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            sudo systemctl start docker
        else
            echo "Error: Unknown OS. Please start Docker manually." >&2
            exit 1
        fi

        echo "Waiting for Docker daemon to initialize..."
        local max_retries=30
        local retry_count=0
        while ! docker info >/dev/null 2>&1; do
            if [ "$retry_count" -ge "$max_retries" ]; then
                echo "Error: Docker failed to start after $max_retries attempts." >&2
                exit 1
            fi
            sleep 2
            retry_count=$((retry_count + 1))
        done
        echo "Docker daemon is now running."
    else
        echo "Docker daemon is already running."
    fi
}

wait_for_engine() {
    case "$1" in
        postgresql)
            until docker exec docker-postgresql-1 pg_isready -U postgres >/dev/null 2>&1; do
                echo "Waiting for PostgreSQL to be ready..."
                sleep 2
            done
            ;;
        mssql)
            until docker exec docker-mssql-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourStrong!Passw0rd' -Q "SELECT 1" -C >/dev/null 2>&1; do
                echo "Waiting for SQL Server to be ready..."
                sleep 5
            done
            ;;
        clickhouse)
            until docker exec docker-clickhouse-1 clickhouse-client --user default --password default -q "SELECT 1" >/dev/null 2>&1; do
                echo "Waiting for ClickHouse to be ready..."
                sleep 2
            done
            ;;
    esac
}

ensure_clickhouse_26() {
    local container_name="docker-clickhouse-1"
    local current_version

    current_version=$(docker exec "$container_name" clickhouse-client --user default --password default -q "SELECT version()" 2>/dev/null | tr -d '\r' || true)
    if [[ "$current_version" != 26.* ]]; then
        echo "ClickHouse version '$current_version' detected. Recreating with 26.x image..."
        docker-compose down
        docker-compose up -d --remove-orphans --force-recreate --build clickhouse
        wait_for_engine clickhouse
    fi
}

ensure_dbprove_built() {
    echo "Ensuring dbprove is built..."
    if [ -d "$BUILD_DIR" ]; then
        cmake --build "$BUILD_DIR" --target dbprove
    else
        if [ -f "$PROJECT_ROOT/CMakePresets.json" ]; then
            cmake --preset osx-arm-base
            cmake --build "$BUILD_DIR" --target dbprove
        else
            echo "Error: Build directory $BUILD_DIR not found and no CMakePresets.json is available." >&2
            exit 1
        fi
    fi

    if [ ! -f "$DBPROVE_PATH" ]; then
        DBPROVE_PATH=$(find "$PROJECT_ROOT/out" -type f -name dbprove -perm -111 | head -n 1)
    fi

    if [ -z "$DBPROVE_PATH" ] || [ ! -f "$DBPROVE_PATH" ]; then
        echo "Error: dbprove executable not found even after the build attempt." >&2
        exit 1
    fi
}

start_engine_if_needed() {
    local engine="$1"
    local service_name
    service_name=$(engine_service_name "$engine")

    if [ -z "$service_name" ]; then
        echo "Note: Engine '$engine' does not have a local Docker container managed by this script."
        return
    fi

    ensure_docker_running

    mkdir -p "$MOUNT_DIR/$service_name"
    cd "$DOCKER_DIR"

    local container_running
    container_running=$(docker-compose ps -q "$service_name" 2>/dev/null | xargs docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")

    if [ "$RESET_FLAG" = true ] || [ "$container_running" != "true" ]; then
        echo "Starting/Restarting Docker container for $service_name..."
        docker-compose down
        if [ "$service_name" = "clickhouse" ]; then
            docker-compose up -d --remove-orphans --force-recreate --build "$service_name"
        else
            docker-compose up -d --remove-orphans --force-recreate "$service_name"
        fi
    else
        echo "Docker container for $service_name is already running. Using existing container."
        local other_services
        other_services=$(docker-compose ps --services 2>/dev/null | grep -v "^$service_name$" || true)
        if [ -n "$other_services" ]; then
            echo "Stopping other services: $other_services"
            docker-compose stop $other_services
        fi
        if [ "$service_name" = "clickhouse" ]; then
            ensure_clickhouse_26
        fi
    fi

    wait_for_engine "$service_name"
}

run_engine() {
    local engine="$1"

    if engine_requires_gcs "$engine"; then
        echo
        echo "--- Checking prerequisites for $engine ---"
        ensure_gcloud_access
    fi

    if [ "$engine" = "databricks" ]; then
        if ! engine_requires_gcs "$engine"; then
            echo
            echo "--- Checking prerequisites for $engine ---"
        fi
        ensure_databricks_access
    fi

    echo
    echo "=== Running benchmark for $engine ==="

    start_engine_if_needed "$engine"

    echo "Running dbprove for $engine..."
    cd "$RUN_DIR"
    local dbprove_args=("-T" "PLAN" "-e" "$engine")
    if [ -n "$VERBOSE_FLAG" ]; then
        dbprove_args=("$VERBOSE_FLAG" "${dbprove_args[@]}")
    fi
    "$DBPROVE_PATH" "${dbprove_args[@]}"

    echo "Benchmark completed for $engine."
}

source_env_if_present

while getopts "vre:" opt; do
    case "$opt" in
        v)
            VERBOSE_FLAG="-v"
            ;;
        r)
            RESET_FLAG=true
            ;;
        e)
            ENGINE_PARAM="$OPTARG"
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND - 1))

if [ -z "$ENGINE_PARAM" ]; then
    usage
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
    export ODBCSYSINI=/opt/homebrew/etc
fi

SELECTED_ENGINES=()
NORMALIZED_ENGINE=$(normalize_engine "$ENGINE_PARAM")
if [ "$NORMALIZED_ENGINE" = "ALL" ]; then
    SELECTED_ENGINES=("${SUPPORTED_ENGINES[@]}")
else
    SELECTED_ENGINES=("$NORMALIZED_ENGINE")
fi

ensure_dbprove_built

for engine in "${SELECTED_ENGINES[@]}"; do
    run_engine "$engine"
done

echo
echo "Benchmark script completed."
