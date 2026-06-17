# PostgreSQL AWS Native

This image is the AWS-oriented PostgreSQL container for native local table storage.

## Model

- The runtime image is based on `Ubuntu 22.04`.
- An EC2 instance mounts ephemeral NVMe storage into the container at `/mnt/nvme`.
- PostgreSQL stores its cluster data under `/mnt/nvme/postgresql/data` by default.
- The container includes a prebuilt `dbprove` binary at `/opt/dbprove/bin/dbprove`.
- `dbprove` is expected to be invoked interactively over a terminal session inside the container.
- Any S3 download or staging work should be performed explicitly through `dbprove` or operator commands.

## Optional environment

- `POSTGRES_NVME_ROOT`
  Defaults to `/mnt/nvme`.
- `PGDATA`
  Defaults to `/mnt/nvme/postgresql/data`.
- `POSTGRES_DB`
  Defaults to `postgres`.
- `POSTGRES_USER`
  Defaults to `postgres`.
- `POSTGRES_PORT`
  Defaults to `5432`.
