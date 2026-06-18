# MSSQL AWS Native

This image is the AWS-oriented SQL Server container for native local table
storage.

## Model

- The runtime image is based on `Ubuntu 22.04`.
- SQL Server on Linux is treated as an `amd64` image target, so build this container with `docker buildx build --platform=linux/amd64`.
- An EC2 instance mounts ephemeral NVMe storage into the container at `/mnt/nvme`.
- The container includes a prebuilt `dbprove` binary at `/opt/dbprove/bin/dbprove`.
- `dbprove` is expected to be invoked interactively over a terminal session inside the container.
- Any S3 download or staging work should be performed explicitly through `dbprove` or operator commands.
- SQL Server stores new database files under `/mnt/nvme/mssql/data` and log files under `/mnt/nvme/mssql/log`.
- The image includes `msodbcsql18`, `mssql-tools18`, and `sqlcmd` so local health checks and in-container diagnostics work the same way as the existing SQL Server tooling.
- The container comes up with the repo default SQL Server login: user `sa`, password `YourStrong!Passw0rd`.

## Optional environment

- `MSSQL_NVME_ROOT`
  Defaults to `/mnt/nvme`.
- `MSSQL_ROOT`
  Defaults to `/mnt/nvme/mssql`.
- `MSSQL_DATA_DIR`
  Defaults to `/mnt/nvme/mssql/data`.
- `MSSQL_LOG_DIR`
  Defaults to `/mnt/nvme/mssql/log`.
- `MSSQL_BACKUP_DIR`
  Defaults to `/mnt/nvme/mssql/backup`.
- `MSSQL_DUMP_DIR`
  Defaults to `/mnt/nvme/mssql/dump`.
- `MSSQL_TCP_PORT`
  Defaults to `1433`.
- `MSSQL_PID`
  Defaults to `Developer`.
- `MSSQL_USER`
  Defaults to `sa`.
- `MSSQL_SA_PASSWORD`
  Defaults to `YourStrong!Passw0rd`.

## Build

Build a matching `amd64` `dbprove` artifact image first:

```bash
docker buildx build \
  --platform=linux/amd64 \
  -f docker/ubuntu/dbprove-prebuilt/Dockerfile \
  -t dbprove-prebuilt:linux-amd64 \
  --load .
```

Then build the SQL Server runtime image against that artifact:

```bash
docker buildx build \
  --platform=linux/amd64 \
  --build-arg DBPROVE_PREBUILT_IMAGE=dbprove-prebuilt:linux-amd64 \
  -f docker/mssql/aws/native/Dockerfile \
  -t dbprove-mssql-aws-native:latest \
  --load .
```
