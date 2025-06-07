find_path(CLICKHOUSE_INCLUDE_DIR clickhouse/client.h)
find_library(CLICKHOUSE_LIB NAMES clickhouse-cpp-lib clickhouse_client)

add_library(clickhouse UNKNOWN IMPORTED GLOBAL)
set_target_properties(clickhouse PROPERTIES
        IMPORTED_LOCATION "${CLICKHOUSE_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${CLICKHOUSE_INCLUDE_DIR}"
)

print_target_info(clickhouse)

