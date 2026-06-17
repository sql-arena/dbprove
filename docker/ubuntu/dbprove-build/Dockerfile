FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_ROOT=/src/extern/vcpkg
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_MAX_CONCURRENCY=4
ENV CMAKE_BUILD_PARALLEL_LEVEL=4
ENV CC=/usr/bin/gcc-13
ENV CXX=/usr/bin/g++-13

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
        gnupg \
        libcurl4-openssl-dev \
        libltdl-dev \
        libssl-dev \
        libtool \
        linux-libc-dev \
        ninja-build \
        pkg-config \
        python3 \
        python3-pip \
        software-properties-common \
        tar \
        unixodbc-dev \
        unzip \
        zip \
        zlib1g-dev \
    && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc-13 \
        g++-13 \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir "cmake>=3.24"

COPY docker/ubuntu/dbprove-build/build-dbprove.sh /usr/local/bin/build-dbprove.sh
RUN chmod +x /usr/local/bin/build-dbprove.sh

WORKDIR /src

ENTRYPOINT ["/usr/local/bin/build-dbprove.sh"]
