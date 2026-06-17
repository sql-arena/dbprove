FROM ubuntu:24.04 AS build

ARG DUCKDB_VERSION=1.5.0

ENV DEBIAN_FRONTEND=noninteractive
ENV DUCKDB_PREBUILT_ROOT=/opt/duckdb-prebuilt
ENV VCPKG_ROOT=/src/extern/vcpkg
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_MAX_CONCURRENCY=4
ENV CMAKE_BUILD_PARALLEL_LEVEL=4

RUN printf 'Acquire::Retries "10";\nAcquire::http::Timeout "30";\n' > /etc/apt/apt.conf.d/80-retries \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        autoconf \
        autoconf-archive \
        automake \
        bash \
        bison \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        flex \
        git \
        libcurl4-openssl-dev \
        libssl-dev \
        libtool \
        linux-libc-dev \
        ninja-build \
        pkg-config \
        python3 \
        tar \
        unixodbc-dev \
        unzip \
        zip \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p "${DUCKDB_PREBUILT_ROOT}/include" "${DUCKDB_PREBUILT_ROOT}/lib" /tmp/duckdb \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-src.zip" -o /tmp/duckdb/libduckdb-src.zip \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-arm64.zip" -o /tmp/duckdb/libduckdb-linux-arm64.zip \
    && curl -fL "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/static-libs-linux-arm64.zip" -o /tmp/duckdb/static-libs-linux-arm64.zip \
    && unzip -j /tmp/duckdb/libduckdb-src.zip duckdb.hpp duckdb.h duckdb_extension.h -d "${DUCKDB_PREBUILT_ROOT}/include" \
    && unzip -j /tmp/duckdb/libduckdb-linux-arm64.zip libduckdb.so libduckdb_static.a -d "${DUCKDB_PREBUILT_ROOT}/lib" \
    && unzip -jo /tmp/duckdb/static-libs-linux-arm64.zip '*.a' -d "${DUCKDB_PREBUILT_ROOT}/lib" \
    && rm -rf /tmp/duckdb

WORKDIR /src

COPY . /src

RUN /src/extern/vcpkg/bootstrap-vcpkg.sh -disableMetrics \
    && cmake -S /src -B /src/out/build/ubuntu-arm64-release-prebuilt-duckdb -G Ninja \
       -DCMAKE_BUILD_TYPE=Release \
       -DCMAKE_TOOLCHAIN_FILE=/src/extern/vcpkg/scripts/buildsystems/vcpkg.cmake \
       -DDBPROVE_DUCKDB_ONLY=ON \
       -DDBPROVE_ENABLE_TESTING=OFF \
       -DVCPKG_TARGET_TRIPLET=arm64-linux-release \
       -DVCPKG_MANIFEST_MODE=ON \
       -DVCPKG_MANIFEST_DIR=/src/docker/duckdb/local/iceberg \
       -DVCPKG_INSTALLED_DIR=/src/out/vcpkg_installed_ubuntu_arm64_prebuilt_duckdb \
       -DVCPKG_OVERLAY_PORTS=/src/docker/duckdb/local/iceberg/vcpkg_overlays \
       -DVCPKG_OVERLAY_TRIPLETS=/src/overlay-triplets \
    && cmake --build /src/out/build/ubuntu-arm64-release-prebuilt-duckdb --target dbprove -j4

FROM build AS runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/opt/dbprove/vcpkg_installed/lib:/opt/dbprove/vcpkg_installed/lib64

RUN mkdir -p /opt/dbprove/bin /opt/dbprove/vcpkg_installed /opt/tpch /repo \
    && cp /src/out/build/ubuntu-arm64-release-prebuilt-duckdb/src/dbprove/dbprove /opt/dbprove/bin/dbprove \
    && cp -R /src/out/vcpkg_installed_ubuntu_arm64_prebuilt_duckdb/arm64-linux-release/. /opt/dbprove/vcpkg_installed/ \
    && cp -R /src/docker/datafusion/local/iceberg/tpch/. /opt/tpch/ \
    && cp /src/docker/duckdb/local/iceberg/entrypoint.sh /usr/local/bin/duckdb-entrypoint.sh \
    && chmod +x /usr/local/bin/duckdb-entrypoint.sh

WORKDIR /repo

ENTRYPOINT ["/usr/local/bin/duckdb-entrypoint.sh"]
