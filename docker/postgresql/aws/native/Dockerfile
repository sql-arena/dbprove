ARG DBPROVE_PREBUILT_IMAGE=dbprove-prebuilt:latest
FROM ${DBPROVE_PREBUILT_IMAGE} AS dbprove-prebuilt

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV POSTGRES_NVME_ROOT=/mnt/nvme
ENV PGDATA=/mnt/nvme/postgresql/data
ENV PATH=/opt/dbprove/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        postgresql \
        postgresql-contrib \
        procps \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/dbprove/bin /mnt/nvme \
    && chown -R postgres:postgres /var/lib/postgresql /mnt/nvme

COPY --from=dbprove-prebuilt /opt/dbprove /opt/dbprove
COPY docker/postgresql/aws/native/conf/benchmark.conf /etc/postgresql/benchmark.conf
COPY --chmod=755 docker/postgresql/aws/native/entrypoint.sh /usr/local/bin/postgresql-entrypoint.sh

RUN chmod +x /opt/dbprove/bin/dbprove

EXPOSE 5432

ENTRYPOINT ["/usr/local/bin/postgresql-entrypoint.sh"]
