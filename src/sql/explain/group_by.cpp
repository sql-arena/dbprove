#include "group_by.h"
#include "glyphs.h"
static constexpr const char* symbol_ = "γ";

namespace sql::explain {
std::string GroupBy::strategyName() const {
  switch (strategy) {
    case Strategy::HASH:
      return "hash";
    case Strategy::SORT_MERGE:
      return "sort";
    case Strategy::PARTIAL:
      return "partial";
    case Strategy::SIMPLE:
      return "simple";
    default:
      return "unknown";
  }
}

std::string GroupBy::compactSymbolic() const {
  std::string result;
  result += symbol_;
  result += "(";
  result += strategyName();
  result += ")";
  result += "{";
  result += join(group_keys, ", ");
  result += " ; ";
  result += join(aggregates, ", ");
  result += "}";

  return result;
}

std::string GroupBy::renderMuggle(size_t max_width) const {
  std::string result = "GROUP BY ";
  result += to_upper(strategyName());
  const auto first_aggregate_size = group_keys.size() > 0 ? group_keys[0].name.size() : 0;
  result += join(group_keys, ", ", max_width - first_aggregate_size);
  result += " AGGREGATE ";
  max_width -= result.size();
  result += join(aggregates, ", ", max_width - 1);
  return result;
}
}