ARG DBPROVE_PREBUILT_IMAGE=dbprove-prebuilt:latest
FROM ${DBPROVE_PREBUILT_IMAGE} AS dbprove-prebuilt

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV MSSQL_NVME_ROOT=/mnt/nvme
ENV MSSQL_ROOT=${MSSQL_NVME_ROOT}/mssql
ENV MSSQL_DATA_DIR=${MSSQL_ROOT}/data
ENV MSSQL_LOG_DIR=${MSSQL_ROOT}/log
ENV MSSQL_BACKUP_DIR=${MSSQL_ROOT}/backup
ENV MSSQL_DUMP_DIR=${MSSQL_ROOT}/dump
ENV MSSQL_TCP_PORT=1433
ENV MSSQL_PID=Developer
ENV MSSQL_USER=sa
ENV ACCEPT_EULA=Y
ENV PATH=/opt/dbprove/bin:/opt/mssql-tools18/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/dbprove/vcpkg_installed/lib:/opt/dbprove/vcpkg_installed/lib64

RUN test "$(dpkg --print-architecture)" = "amd64" \
    || (echo "mssql/aws/native requires an amd64 build target; use docker buildx build --platform=linux/amd64" >&2 && exit 1)

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        gnupg \
        libgcc-s1 \
        libstdc++6 \
        procps \
        software-properties-common \
        unixodbc \
        zlib1g \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc-13-base \
        libstdc++6 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null \
    && curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list \
        | tee /etc/apt/sources.list.d/mssql-server-2022.list >/dev/null \
    && curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
        | tee /etc/apt/sources.list.d/mssql-release.list >/dev/null \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        mssql-server \
        mssql-tools18 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/dbprove "${MSSQL_NVME_ROOT}" "${MSSQL_DATA_DIR}" "${MSSQL_LOG_DIR}" "${MSSQL_BACKUP_DIR}" "${MSSQL_DUMP_DIR}"

COPY --from=dbprove-prebuilt /opt/dbprove /opt/dbprove
COPY docker/mssql/local/native/conf/mssql.conf /var/opt/mssql/mssql.conf
COPY --chmod=755 docker/mssql/aws/native/entrypoint.sh /usr/local/bin/mssql-entrypoint.sh

RUN chmod +x /opt/dbprove/bin/dbprove

EXPOSE 1433

ENTRYPOINT ["/usr/local/bin/mssql-entrypoint.sh"]
