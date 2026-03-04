# Docker Infrastructure

This directory contains Dockerfiles and configurations to start databases for benchmarking.

## PostgreSQL
- `postgresql/` - PostgreSQL configuration and setup (includes a benchmark-tuned config based on `postgres:latest`).

### Build
```bash
cd docker/postgresql
docker build -t sql-arena-postgres:latest .
```

### Run (example)
```bash
docker run --rm \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=bench \
  -p 5432:5432 \
  sql-arena-postgres:latest
```

Notes:
- The image uses unsafe settings for durability (e.g., `fsync=off`) to maximize performance. For benchmarking only.
- To customize memory-related settings, edit `docker/postgresql/conf/benchmark.conf` and rebuild.
