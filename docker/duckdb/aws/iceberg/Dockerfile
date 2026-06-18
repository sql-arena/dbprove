ARG DBPROVE_PREBUILT_IMAGE=dbprove-prebuilt:latest
FROM ${DBPROVE_PREBUILT_IMAGE} AS dbprove-prebuilt

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV DUCKDB_NVME_ROOT=/mnt/nvme
ENV DUCKDB_HOME=/mnt/nvme/duckdb
ENV PATH=/opt/dbprove/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/dbprove/vcpkg_installed/lib:/opt/dbprove/vcpkg_installed/lib64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        gnupg \
        libgcc-s1 \
        libstdc++6 \
        procps \
        software-properties-common \
        zlib1g \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc-13-base \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/dbprove /mnt/nvme /mnt/nvme/duckdb

COPY --from=dbprove-prebuilt /opt/dbprove /opt/dbprove
COPY --chmod=755 docker/duckdb/aws/iceberg/entrypoint.sh /usr/local/bin/duckdb-entrypoint.sh

RUN chmod +x /opt/dbprove/bin/dbprove

WORKDIR /mnt/nvme/duckdb

ENTRYPOINT ["/usr/local/bin/duckdb-entrypoint.sh"]
CMD ["bash"]
