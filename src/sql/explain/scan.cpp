#include "scan.h"
static constexpr auto symbol_ = "📄";

std::string sql::explain::Scan::compactSymbolic() const {
  return table_name;
}

std::string sql::explain::Scan::renderMuggle(size_t max_width) const {
  std::string result = "SCAN " + table_name;
  return result;
}