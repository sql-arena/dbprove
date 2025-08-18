#include "group_by.h"
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
  result += Column::join(group_keys, ", ");
  result += " ; ";
  result += Column::join(aggregates, ", ");
  result += "}";

  return result;
}

std::string GroupBy::renderMuggle(size_t max_width) const {
  std::string result = "GROUP BY ";
  result += to_upper(strategyName());
  result += " (";
  const auto first_group_size = aggregates.size() > 0 ? aggregates[0].name.size() : 0;
  const auto first_aggregate_size = group_keys.size() > 0 ? group_keys[0].name.size() : 0;
  const auto reserve_width = first_group_size + first_aggregate_size + 50 + result.size();
  result += Column::join(group_keys, ", ", max_width - reserve_width);
  result += ")";
  result += " AGGREGATE (";
  result += Column::join(aggregates, ", ", max_width - result.size() - 1);
  result += ")";
  return result;
}
}