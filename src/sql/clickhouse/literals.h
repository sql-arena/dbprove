#pragma once
#include <string>

namespace sql::clickhouse {
/**
 * Normalize ClickHouse typed literal forms to canonical SQL literals.
 *
 * Examples:
 * - 15_UInt16 -> 15
 * - UInt64_3 -> 3
 * - _CAST(4979.8198 Float64, 'Float64') -> 4979.8198
 * - String_'AIR' -> 'AIR'
 */
std::string stripClickHouseTypedLiterals(std::string input);
}
