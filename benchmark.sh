#!/bin/bash

# Exit on error
set -e

# Configuration
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"
RUN_DIR="$PROJECT_ROOT/run"
MOUNT_DIR="$RUN_DIR/mount"

# Display usage
usage() {
    echo "Usage: $0 [-v] [-r] -e <engine>"
    echo "  -v: Enable verbose output (passed to dbprove)"
    echo "  -r: Force restart of the Docker container (reset)"
    echo "  -e: Specify the engine (required)"
    echo "Supported engines (with Docker): postgresql, clickhouse, mssql, trino"
    echo "Other engines (external/file): sqlite, duckdb, databricks, yellowbrick, mariadb"
    exit 1
}

# Set ODBC environment variables for macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    export ODBCSYSINI=/opt/homebrew/etc
fi

# Ensure Docker daemon is running
ensure_docker_running() {
    local engine=$1
    echo "Checking if Docker daemon is running..."
    if ! docker info >/dev/null 2>&1; then
        echo "Docker daemon not running. Attempting to start Docker..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            open --background -a Docker
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            sudo systemctl start docker
        else
            echo "Error: Unknown OS. Please start Docker manually."
            exit 1
        fi

        # Wait for Docker to start
        echo "Waiting for Docker daemon to initialize..."
        MAX_RETRIES=30
        RETRY_COUNT=0
        while ! docker info >/dev/null 2>&1; do
            if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
                echo "Error: Docker failed to start after $MAX_RETRIES seconds."
                exit 1
            fi
            sleep 2
            RETRY_COUNT=$((RETRY_COUNT + 1))
        done
        echo "Docker daemon is now running."
    else
        echo "Docker daemon is already running."
    fi
}

# Wait for the engine to be ready
wait_for_engine() {
    local engine=$1
    case $engine in
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
    esac
}

# Check for parameter
VERBOSE_FLAG=""
ENGINE_PARAM=""
RESET_FLAG=false
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
shift $((OPTIND-1))

if [ -z "$ENGINE_PARAM" ]; then
    usage
fi

# Build dbprove if it needs building
BUILD_DIR="$PROJECT_ROOT/out/build/osx-arm-base"
DBPROVE_PATH="$BUILD_DIR/src/dbprove/dbprove"

echo "Ensuring dbprove is built..."
if [ -d "$BUILD_DIR" ]; then
    cmake --build "$BUILD_DIR" --target dbprove
else
    # If the default build directory doesn't exist, try to use presets if available
    if [ -f "$PROJECT_ROOT/CMakePresets.json" ]; then
        cmake --preset osx-arm-base
        cmake --build "$BUILD_DIR" --target dbprove
    else
        echo "Error: Build directory $BUILD_DIR not found and no CMakePresets.json available."
        exit 1
    fi
fi

if [ ! -f "$DBPROVE_PATH" ]; then
    # Fallback search if the path above is incorrect or differs
    DBPROVE_PATH=$(find "$PROJECT_ROOT/out" -name dbprove -type f -perm +111 | head -n 1)
fi

if [ -z "$DBPROVE_PATH" ] || [ ! -f "$DBPROVE_PATH" ]; then
    echo "Error: dbprove executable not found even after build attempt."
    exit 1
fi

ENGINE_INPUT=$(echo "$ENGINE_PARAM" | tr '[:upper:]' '[:lower:]')

# Map script engine parameters to docker-compose service names
# Based on src/sql/engine.cpp known_names
case "$ENGINE_INPUT" in
    postgresql|postgres|pg)
        SERVICE_NAME="postgresql"
        ;;
    clickhouse|ch)
        SERVICE_NAME="clickhouse"
        ;;
    sqlserver|mssql|"sql server")
        SERVICE_NAME="mssql"
        ;;
    trino)
        SERVICE_NAME="trino"
        ;;
    # Engines that don't have a docker service in docker-compose.yml yet
    sqlite|sqlite3|duckdb|duck|databricks|yellowbrick|yb|ybd|mariadb|mysql|utopia)
        SERVICE_NAME=""
        ;;
    *)
        echo "Error: Engine '$ENGINE_INPUT' not recognized."
        usage
        ;;
esac

# Start the docker container if a service name exists
if [ -n "$SERVICE_NAME" ]; then
    # Ensure Docker daemon is running
    ensure_docker_running "$SERVICE_NAME"

    # Ensure mount point exists
    ENGINE_MOUNT_POINT="$MOUNT_DIR/$SERVICE_NAME"
    mkdir -p "$ENGINE_MOUNT_POINT"

    cd "$DOCKER_DIR"
    
    # Check if the container for the service is already running
    CONTAINER_RUNNING=$(docker-compose ps -q "$SERVICE_NAME" 2>/dev/null | xargs docker inspect -f '{{.State.Running}}' 2>/dev/null || echo "false")
    
    if [ "$RESET_FLAG" = true ] || [ "$CONTAINER_RUNNING" != "true" ]; then
        echo "Starting/Restarting Docker container for $SERVICE_NAME..."
        # Shut down any existing containers from this project to ensure only one engine runs at a time
        docker-compose down
        # Use --force-recreate to handle any name conflicts and ensure a fresh start
        docker-compose up -d --remove-orphans --force-recreate "$SERVICE_NAME"
    else
        echo "Docker container for $SERVICE_NAME is already running. Using existing container."
        OTHER_SERVICES=$(docker-compose ps --services 2>/dev/null | grep -v "^$SERVICE_NAME$" || true)
        if [ -n "$OTHER_SERVICES" ]; then
            echo "Stopping other services: $OTHER_SERVICES"
            docker-compose stop $OTHER_SERVICES
        fi
    fi
    # Always wait for the engine to be ready to ensure dbprove can connect
    wait_for_engine "$SERVICE_NAME"
else
    echo "Note: Engine '$ENGINE_INPUT' does not have a local Docker container managed by this script."
    # We still proceed to run dbprove as it might be an external engine or file-based
fi

# Run dbprove with -T PLAN in run/ directory
echo "Running dbprove for $ENGINE_INPUT..."
cd "$RUN_DIR"
"$DBPROVE_PATH" $VERBOSE_FLAG -T PLAN -e "$ENGINE_INPUT"

echo "Benchmark script completed for $ENGINE_INPUT."
