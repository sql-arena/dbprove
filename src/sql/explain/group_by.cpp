#include "group_by.h"
static constexpr const char* symbol_ = "γ";

std::string sql::explain::GroupBy::strategyName() const {
  switch (strategy) {
    case Strategy::HASH:
      return "hash";
    case Strategy::SORT_MERGE:
      return "sort";
    case Strategy::PARTIAL:
      return "partial";
    default:
      return "unknown";
  }

}

std::string sql::explain::GroupBy::compactSymbolic() const {
  std::string result;
  result += symbol_;
  result += "(";
  result += strategyName();
  result += ")";
  result += "{";
  result += Column::join(group_keys, ", ");
  result += " ; ";
  result+= Column::join(aggregates, ", ");
  result += "}";

  return result;
}

std::string sql::explain::GroupBy::renderMuggle() const {
  std::string result = "GROUP BY ";
  result += to_upper(strategyName());
  result += " (";
  result += Column::join(group_keys, ", ");
  result += ")";
  result += " AGGREGATE (";
  result+= Column::join(aggregates, ", ");
  result+=")";
  return result;
}