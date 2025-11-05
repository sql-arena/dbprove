#pragma once
#include <dbprove/sql/sql_type.h>

namespace sql::explain {
/**
 * During planning, row values can get to nearly infinite. But we need integers for return values
 * @param plan_rows Rows from query plan
 * @return Safe RowCount
 */
inline RowCount cutoff(const double plan_rows) {
  if (std::isnan(plan_rows)) {
    return -1;
  }
  if (plan_rows > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
    return std::numeric_limits<uint64_t>::max();
  }
  return static_cast<RowCount>(plan_rows);
}
}