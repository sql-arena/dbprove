ARG DBPROVE_BUILD_IMAGE=dbprove-ubuntu-build:latest
FROM ${DBPROVE_BUILD_IMAGE} AS build

ARG DUCKDB_VERSION=1.5.0

ENV DEBIAN_FRONTEND=noninteractive
ENV DUCKDB_PREBUILT_ROOT=/opt/duckdb-prebuilt
ENV LD_LIBRARY_PATH=/opt/dbprove/vcpkg_installed/lib:/opt/dbprove/vcpkg_installed/lib64

WORKDIR /src

COPY . /src

RUN case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
      amd64) duckdb_arch=x64 ;; \
      arm64) duckdb_arch=arm64 ;; \
      *) echo "Unsupported architecture: ${TARGETARCH:-unknown}" >&2; exit 1 ;; \
    esac \
    && mkdir -p "${DUCKDB_PREBUILT_ROOT}/include" "${DUCKDB_PREBUILT_ROOT}/lib" /tmp/duckdb \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-src.zip" -o /tmp/duckdb/libduckdb-src.zip \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${duckdb_arch}.zip" -o /tmp/duckdb/libduckdb-linux.zip \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/static-libs-linux-${duckdb_arch}.zip" -o /tmp/duckdb/static-libs-linux.zip \
    && unzip -j /tmp/duckdb/libduckdb-src.zip duckdb.hpp duckdb.h duckdb_extension.h -d "${DUCKDB_PREBUILT_ROOT}/include" \
    && unzip -j /tmp/duckdb/libduckdb-linux.zip libduckdb.so libduckdb_static.a -d "${DUCKDB_PREBUILT_ROOT}/lib" \
    && unzip -jo /tmp/duckdb/static-libs-linux.zip '*.a' -d "${DUCKDB_PREBUILT_ROOT}/lib" \
    && rm -rf /tmp/duckdb \
    && DBPROVE_DUCKDB_ONLY=ON \
       DBPROVE_ENABLE_TESTING=OFF \
       DBPROVE_BUILD_TARGET=dbprove \
       DBPROVE_BUILD_PARALLELISM=4 \
       DBPROVE_BUILD_DIR=/src/out/build/duckdb-local \
       DBPROVE_VCPKG_MANIFEST_DIR=/src/docker/duckdb/local/iceberg \
       DBPROVE_VCPKG_INSTALLED_DIR=/src/out/vcpkg_installed_duckdb_local \
       DBPROVE_VCPKG_OVERLAY_PORTS=/src/docker/duckdb/local/iceberg/vcpkg_overlays \
       /usr/local/bin/build-dbprove.sh /src \
    && case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
         amd64) triplet=x64-linux-release ;; \
         arm64) triplet=arm64-linux-release ;; \
         *) echo "Unsupported architecture: ${TARGETARCH:-unknown}" >&2; exit 1 ;; \
       esac \
    && mkdir -p /opt/dbprove-stage/bin /opt/dbprove-stage/vcpkg_installed \
    && cp /src/out/build/duckdb-local/src/dbprove/dbprove /opt/dbprove-stage/bin/dbprove \
    && cp -R "/src/out/vcpkg_installed_duckdb_local/${triplet}/." /opt/dbprove-stage/vcpkg_installed/

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/opt/dbprove/vcpkg_installed/lib:/opt/dbprove/vcpkg_installed/lib64

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        libgcc-s1 \
        libstdc++6 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/dbprove/bin /opt/dbprove/vcpkg_installed /repo

COPY --from=build /opt/dbprove-stage/bin/dbprove /opt/dbprove/bin/dbprove
COPY --from=build /opt/dbprove-stage/vcpkg_installed/. /opt/dbprove/vcpkg_installed/
COPY docker/duckdb/local/iceberg/entrypoint.sh /usr/local/bin/duckdb-entrypoint.sh

RUN chmod +x /usr/local/bin/duckdb-entrypoint.sh

WORKDIR /repo

ENTRYPOINT ["/usr/local/bin/duckdb-entrypoint.sh"]
