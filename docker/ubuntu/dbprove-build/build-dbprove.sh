#!/usr/bin/env bash
set -euo pipefail

workspace="${1:-/src}"
build_type="${DBPROVE_BUILD_TYPE:-Release}"
duckdb_only="${DBPROVE_DUCKDB_ONLY:-OFF}"
enable_testing="${DBPROVE_ENABLE_TESTING:-OFF}"
target_name="${DBPROVE_BUILD_TARGET:-dbprove}"
parallelism="${DBPROVE_BUILD_PARALLELISM:-4}"
manifest_dir="${DBPROVE_VCPKG_MANIFEST_DIR:-${workspace}}"
overlay_ports="${DBPROVE_VCPKG_OVERLAY_PORTS:-}"
overlay_triplets="${DBPROVE_VCPKG_OVERLAY_TRIPLETS:-${workspace}/overlay-triplets}"
default_overlay_ports="${workspace}/extern/vcpkg_overlays"

case "${TARGETARCH:-$(dpkg --print-architecture)}" in
  amd64)
    triplet_arch="x64"
    ;;
  arm64)
    triplet_arch="arm64"
    ;;
  *)
    echo "Unsupported architecture: ${TARGETARCH:-unknown}" >&2
    exit 1
    ;;
esac

triplet="${DBPROVE_VCPKG_TRIPLET:-${triplet_arch}-linux-release}"
build_dir="${DBPROVE_BUILD_DIR:-${workspace}/out/build/ubuntu-${triplet}-${build_type,,}}"
installed_dir="${DBPROVE_VCPKG_INSTALLED_DIR:-${workspace}/out/vcpkg_installed_${triplet_arch}_linux}"
manifest_file="${manifest_dir}/vcpkg.json"
preset_name=""
compiler_stamp="${installed_dir}/.dbprove-compiler"

if [[ -d "${default_overlay_ports}" ]]; then
  if [[ -n "${overlay_ports}" ]]; then
    overlay_ports="${default_overlay_ports};${overlay_ports}"
  else
    overlay_ports="${default_overlay_ports}"
  fi
fi

cd "${workspace}"

current_compiler_signature="$(
  {
    printf 'CC=%s\n' "${CC:-}"
    printf 'CXX=%s\n' "${CXX:-}"
    if [[ -n "${CC:-}" ]] && command -v "${CC}" >/dev/null 2>&1; then
      "${CC}" --version | head -n 1
    fi
    if [[ -n "${CXX:-}" ]] && command -v "${CXX}" >/dev/null 2>&1; then
      "${CXX}" --version | head -n 1
    fi
  } | tr '\n' '|'
)"

if [[ -d "${installed_dir}" ]]; then
  previous_compiler_signature=""
  if [[ -f "${compiler_stamp}" ]]; then
    previous_compiler_signature="$(<"${compiler_stamp}")"
  fi

  if [[ "${previous_compiler_signature}" != "${current_compiler_signature}" ]]; then
    echo "Compiler changed for ${installed_dir}; clearing cached vcpkg install tree." >&2
    rm -rf "${installed_dir}"
  fi
fi

if [[ -f "${manifest_file}" ]]; then
  baseline="$(
    python3 - <<'PY' "${manifest_file}"
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    print(json.load(handle).get("builtin-baseline", ""))
PY
  )"

  if [[ -n "${baseline}" ]] && ! git -C "${workspace}/extern/vcpkg" cat-file -e "${baseline}^{commit}" >/dev/null 2>&1; then
    rm -rf "${workspace}/extern/vcpkg"
    git clone https://github.com/microsoft/vcpkg.git "${workspace}/extern/vcpkg"

    if ! git -C "${workspace}/extern/vcpkg" cat-file -e "${baseline}^{commit}" >/dev/null 2>&1; then
      git -C "${workspace}/extern/vcpkg" fetch origin "${baseline}"
    fi
  fi
fi

"${workspace}/extern/vcpkg/bootstrap-vcpkg.sh" -disableMetrics

export VCPKG_ROOT="${workspace}/extern/vcpkg"
export VCPKG_OVERLAY_TRIPLETS="${overlay_triplets}"
if [[ -n "${overlay_ports}" ]]; then
  export VCPKG_OVERLAY_PORTS="${overlay_ports}"
fi

if [[ "${triplet}" == "arm64-linux-release" && "${build_type}" == "Release" ]]; then
  if [[ "${duckdb_only}" == "ON" && "${manifest_dir}" == "${workspace}/docker/duckdb" ]]; then
    preset_name="ubuntu-arm64-release-prebuilt-duckdb"
  elif [[ "${duckdb_only}" == "OFF" && "${manifest_dir}" == "${workspace}" ]]; then
    preset_name="ubuntu-arm64-release"
  fi
fi

if [[ -n "${preset_name}" ]]; then
  cmake --preset "${preset_name}"
  cmake --build "${workspace}/out/build/${preset_name}" --target "${target_name}" -j"${parallelism}"
  mkdir -p "${installed_dir}"
  printf '%s' "${current_compiler_signature}" > "${compiler_stamp}"
  exit 0
fi

cmake_args=(
  -S "${workspace}"
  -B "${build_dir}"
  -G Ninja
  -DCMAKE_BUILD_TYPE="${build_type}"
  -DCMAKE_TOOLCHAIN_FILE="${workspace}/extern/vcpkg/scripts/buildsystems/vcpkg.cmake"
  -DDBPROVE_ENABLE_TESTING="${enable_testing}"
  -DDBPROVE_DUCKDB_ONLY="${duckdb_only}"
  -DVCPKG_TARGET_TRIPLET="${triplet}"
  -DVCPKG_MANIFEST_MODE=ON
  -DVCPKG_MANIFEST_DIR="${manifest_dir}"
  -DVCPKG_INSTALLED_DIR="${installed_dir}"
  -DVCPKG_OVERLAY_TRIPLETS="${overlay_triplets}"
)

if [[ -n "${overlay_ports}" ]]; then
  cmake_args+=(-DVCPKG_OVERLAY_PORTS="${overlay_ports}")
fi

cmake "${cmake_args[@]}"

cmake --build "${build_dir}" --target "${target_name}" -j"${parallelism}"
mkdir -p "${installed_dir}"
printf '%s' "${current_compiler_signature}" > "${compiler_stamp}"
