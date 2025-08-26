#include "scan.h"

#include "glyphs.h"
static constexpr auto symbol_ = "📄";

std::string sql::explain::Scan::compactSymbolic() const {
  return table_name;
}

std::string sql::explain::Scan::renderMuggle(size_t max_width) const {
  std::string result = "SCAN " + table_name;
  if (!filter_condition.empty()) {
    result += " WHERE ";
    max_width -= result.size();
    result += ellipsify(filter_condition, max_width);
  }
  return result;
}