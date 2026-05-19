vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO google/cityhash
    REF 8af9b8c2b889d80c22d6bc26ba0df1afb79a30db
    SHA512 5878a6a4f8ee99593412d446d96c05be1f89fa7771eca49ff4a52ce181de8199ba558170930996d36f6df80a65889d93c81ab2611868b015d8db913e2ecd2eb9
    HEAD_REF master
)

file(COPY "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" DESTINATION "${SOURCE_PATH}")

# The shipped config.h is sufficient for our supported container targets and
# avoids an autotools configure step that is fragile on arm64-linux images.
file(COPY "${CMAKE_CURRENT_LIST_DIR}/config.h" DESTINATION "${SOURCE_PATH}/src")

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        "sse"   ENABLE_SSE
)

vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        ${FEATURE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH share/cmake/cityhash)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include"
                    "${CURRENT_PACKAGES_DIR}/debug/share")

configure_file("${SOURCE_PATH}/COPYING" "${CURRENT_PACKAGES_DIR}/share/cityhash/copyright" COPYONLY)
