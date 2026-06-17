ARG DBPROVE_BUILD_IMAGE=dbprove-ubuntu-build:latest
ARG DBPROVE_GIT_URL=https://github.com/sql-arena/dbprove.git
ARG DBPROVE_GIT_REF=main
FROM ${DBPROVE_BUILD_IMAGE} AS build

WORKDIR /src
ARG DBPROVE_GIT_URL
ARG DBPROVE_GIT_REF
RUN rm -rf /src \
    && git init /src \
    && git -C /src remote add origin "${DBPROVE_GIT_URL}" \
    && git -C /src fetch --depth 1 origin "${DBPROVE_GIT_REF}" \
    && git -C /src checkout -f FETCH_HEAD
RUN DBPROVE_ENABLE_TESTING=OFF /usr/local/bin/build-dbprove.sh /src \
    && case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
         amd64) triplet=x64-linux-release ;; \
         arm64) triplet=arm64-linux-release ;; \
         *) echo "Unsupported architecture: ${TARGETARCH:-unknown}" >&2; exit 1 ;; \
       esac \
    && if [ -d "/src/out/vcpkg_installed_ubuntu_arm64/${triplet}" ]; then \
         install_root=/src/out/vcpkg_installed_ubuntu_arm64; \
       elif [ -d "/src/out/vcpkg_installed_arm64_linux/${triplet}" ]; then \
         install_root=/src/out/vcpkg_installed_arm64_linux; \
       elif [ -d "/src/out/vcpkg_installed_x64_linux/${triplet}" ]; then \
         install_root=/src/out/vcpkg_installed_x64_linux; \
       else \
         echo "Could not locate vcpkg installed tree for ${triplet}" >&2; exit 1; \
       fi \
    && mkdir -p /opt/dbprove/bin /opt/dbprove/vcpkg_installed \
    && cp /src/out/build/ubuntu-*/src/dbprove/dbprove /opt/dbprove/bin/dbprove \
    && cp -R "${install_root}/${triplet}/." /opt/dbprove/vcpkg_installed/

FROM scratch

COPY --from=build /opt/dbprove /opt/dbprove
