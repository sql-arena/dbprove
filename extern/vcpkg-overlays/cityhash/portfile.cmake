vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
        OUT_SOURCE_PATH SOURCE_PATH
        REPO google/cityhash
        REF 8af9b8c2b889d80c22d6bc26ba0df1afb79a30db
        SHA512 5878a6a4f8ee99593412d446d96c05be1f89fa7771eca49ff4a52ce181de8199ba558170930996d36f6df80a65889d93c81ab2611868b015d8db913e2ecd2eb9
        HEAD_REF master
)

configure_file("${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" "${SOURCE_PATH}/CMakeLists.txt" COPYONLY)
configure_file("${CMAKE_CURRENT_LIST_DIR}/patches/byteswap.h" "${SOURCE_PATH}/src/byteswap.h" COPYONLY)


# Platform-specific handling for byteswap.h
if(VCPKG_TARGET_IS_WINDOWS)
    configure_file("${CMAKE_CURRENT_LIST_DIR}/config.h" "${SOURCE_PATH}/src/config.h" COPYONLY)
else()
    file(MAKE_DIRECTORY "${SOURCE_PATH}/out")
    vcpkg_execute_required_process(
            COMMAND "${SOURCE_PATH}/configure"
            WORKING_DIRECTORY "${SOURCE_PATH}/out"
            LOGNAME configure-${TARGET_TRIPLET}
    )
    configure_file("${SOURCE_PATH}/out/config.h" "${SOURCE_PATH}/src/config.h" COPYONLY)
endif()

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
        FEATURES
        "sse" ENABLE_SSE
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


