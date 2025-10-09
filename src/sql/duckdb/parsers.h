#pragma once
#include <cmath>
#include <cstdint>
#include <string>

#include "dbprove/sql/sql_type.h"
#include <duckdb/common/hugeint.hpp>
#include <duckdb/common/types/hugeint.hpp>

namespace sql::duckdb {
template <typename T>
SqlVariant parseDecimal(T value, uint16_t scale) {
  // We may need to “shift” the value up, so operate on absolute values first.
  std::string str = std::to_string(std::abs(value));

  // Pad with leading zeros if needed
  if (str.length() <= scale) {
    const std::string zeros(scale - str.length() + 1, '0');
    str = zeros + str;
  }

  // Insert decimal point
  if (scale > 0) {
    str.insert(str.length() - scale, ".");
  }

  // Add negative sign if needed
  if (value < 0) {
    str = "-" + str;
  }

  return SqlVariant(SqlDecimal(str));
}

template <>
SqlVariant parseDecimal(::duckdb::hugeint_t value, uint16_t scale) {
  // Get string representation with DuckDB's `Hugeint::ToString`
  std::string str;

  // Check if the value is negative
  const bool isNegative = value < 0;

  // Use DuckDB's own ToString method which handles `hugeint_t` correctly
  str = ::duckdb::Hugeint::ToString(isNegative ? -value : value);

  // Pad with leading zeros if needed
  if (str.length() <= scale) {
    const std::string zeros(scale - str.length() + 1, '0');
    str = zeros + str;
  }

  // Insert decimal point
  if (scale > 0) {
    str.insert(str.length() - scale, ".");
  }

  // Add negative sign if needed
  if (isNegative) {
    str = "-" + str;
  }

  return SqlVariant(SqlDecimal(str));
}
}