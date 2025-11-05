#include "group_by.h"
#include "glyphs.h"
static constexpr const char* symbol_ = "γ";

namespace sql::explain {
GroupBy::GroupBy(const Strategy strategy, const std::vector<Column>& group_keys, const std::vector<Column>& aggregates)
  : Node(NodeType::GROUP_BY)
  , strategy(strategy)
  , group_keys(group_keys)
  , aggregates(aggregates) {
  for (size_t i = 0; i < aggregates.size(); ++i) {
    const auto& agg = aggregates[i];
    aggregateAliases[agg] = "agg_" + std::to_string(i);
  }
}

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
  if (!group_keys.empty()) {
    const auto first_aggregate_size = group_keys.size() > 0 ? group_keys[0].name.size() : 0;
    result += " ";
    result += join(group_keys, ", ", max_width - first_aggregate_size);
  }
  result += " AGGREGATE ";
  max_width -= result.size();
  result += join(aggregates, ", ", max_width - 1);
  return result;
}

std::string GroupBy::treeSQLImpl(const size_t indent) const {
  std::string result = newline(indent);
  result += "(SELECT ";
  result += join(group_keys, ", ");

  auto col_count = group_keys.size();
  // Aggregates must have a name so parent projection nodes can refer to them
  for (auto [c, a] : aggregateAliases) {
    result += col_count++ > 0 ? ", " : "";
    result += c.name + " AS " + a;
  }
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  if (!group_keys.empty()) {
    result += "GROUP BY " + join(group_keys, ", ");
  }
  result += newline(indent);
  result += ") AS " + subquerySQLAlias();
  return result;
}
}