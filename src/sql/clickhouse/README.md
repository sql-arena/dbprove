# ClickHouse Driver

Uses the native ClickHouse protocol to talk with Clickhouse

### Running with Docker

To start a ClickHouse instance for testing:
```bash
cd docker
docker-compose up -d clickhouse
```
Data is persisted in `run/mount/clickhouse`.