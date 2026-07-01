#pragma once
#include <dbprove/sql/sql_type.h>

namespace sql::explain {
/**
 * During planning, row values can get to nearly infinite. But we need integers for return values
 * @param plan_rows Rows from query plan
 * @return Safe RowCount
 */
// Sentinel: one or more actuals were missing (NaN/inf); the aggregate is undefined.
constexpr RowCount ROWS_UNKNOWN = std::numeric_limits<RowCount>::max();

inline RowCount cutoff(const double plan_rows) {
  if (std::isnan(plan_rows)) {
    return ROWS_UNKNOWN;
  }
  if (plan_rows > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
    return ROWS_UNKNOWN;
  }
  return static_cast<RowCount>(plan_rows);
}
}