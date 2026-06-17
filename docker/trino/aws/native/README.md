# Trino AWS Native

This image is the AWS-oriented Trino container for native local table storage.

## Model

- The runtime image is based on `Ubuntu 22.04`.
- An EC2 instance mounts ephemeral NVMe storage into the container at `/mnt/nvme`.
- The container includes a prebuilt `dbprove` binary at `/opt/dbprove/bin/dbprove`.
- `dbprove` is expected to be invoked interactively over a terminal session and handle any S3 download or staging work itself.
- Trino materializes managed ORC tables into the NVMe mount under `warehouse/`.
- The Hive connector uses a standalone Hive metastore running inside the same container and stores its Derby metastore database on the NVMe mount.
- Trino runs with its own Java 25 runtime, while the embedded Hive metastore uses the Java runtime copied from the Hive image.

## Optional environment

- `TRINO_NVME_ROOT`
  Defaults to `/mnt/nvme`.
- `TRINO_METASTORE_PORT`
  Defaults to `9083`.

## Ready signal

When bootstrap completes successfully, the container writes:

- `/tmp/trino-bootstrap-ready`
