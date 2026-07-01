#include "group_by.h"
#include "glyphs.h"
static constexpr const char* symbol_ = "γ";

namespace sql::explain {
GroupBy::GroupBy(const Strategy strategy, const std::vector<Column>& group_keys, const std::vector<Column>& aggregates)
  : Node(NodeType::GROUP_BY)
  , strategy(strategy)
  , group_keys(group_keys)
  , aggregates(aggregates) {
  for (const auto& agg : aggregates) {
    if (agg.hasAlias()) {
      aggregateAliases[agg] = agg.alias;
    }
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
  std::string result;
  if (!aggregates.empty()) {
    std::vector<Column> aliased_aggregates;
    aliased_aggregates.reserve(aggregates.size());
    for (const auto& aggregate : aggregates) {
      auto rendered = aggregate.name;
      if (aggregateAliases.contains(aggregate)) {
        rendered += " AS " + aggregateAliases.at(aggregate);
      }
      aliased_aggregates.emplace_back(rendered);
    }
    result += "AGGREGATE ";
    result += join(aliased_aggregates, ", ");
    if (!group_keys.empty()) {
      result += " ";
    }
  }
  if (!group_keys.empty()) {
    result += "GROUP BY ";
    result += to_upper(strategyName());
    const auto first_aggregate_size = group_keys.size() > 0 ? group_keys[0].name.size() : 0;
    result += " ";
    result += join(group_keys, ", ", max_width - first_aggregate_size);
  }

  return result;
}

// Walk through single-child passthrough nodes to find the first GroupBy descendant.
static const GroupBy* findFirstGroupByDescendant(const Node* node) {
  while (node != nullptr) {
    if (node->type == NodeType::GROUP_BY) {
      return dynamic_cast<const GroupBy*>(node);
    }
    if (node->childCount() != 1) {
      return nullptr;
    }
    node = node->firstChild();
  }
  return nullptr;
}

std::string GroupBy::treeSQLImpl(const size_t indent) const {
  const auto& group_keys_for_sql = synthetic_group_keys.empty() ? group_keys : synthetic_group_keys;
  const auto& aggregates_for_sql = synthetic_aggregates.empty() ? aggregates : synthetic_aggregates;
  const auto& aliases_for_sql = synthetic_aggregateAliases.empty() ? aggregateAliases : synthetic_aggregateAliases;

  // For FINAL aggregates, check if the first GROUP BY descendant is a matching PARTIAL.
  // If so, use pass-through SQL to avoid re-aggregating partial results on a single node.
  // (Distributed PARTIAL/FINAL pairs compute the same result as a single-node GROUP BY.)
  if ((strategy == Strategy::HASH || strategy == Strategy::SORT_MERGE) && childCount() > 0) {
    const GroupBy* partial = findFirstGroupByDescendant(firstChild());
    if (partial && partial->strategy == Strategy::PARTIAL) {
      const auto& partial_keys = partial->synthetic_group_keys.empty()
                                  ? partial->group_keys
                                  : partial->synthetic_group_keys;
      const auto& partial_aggs = partial->synthetic_aggregates.empty()
                                  ? partial->aggregates
                                  : partial->synthetic_aggregates;
      const auto& partial_aliases = partial->synthetic_aggregateAliases.empty()
                                     ? partial->aggregateAliases
                                     : partial->synthetic_aggregateAliases;

      // Keys must match in count and name
      bool keys_match = group_keys_for_sql.size() == partial_keys.size();
      for (size_t i = 0; keys_match && i < group_keys_for_sql.size(); ++i) {
        keys_match = (group_keys_for_sql[i].name == partial_keys[i].name);
      }

      if (keys_match && aggregates_for_sql.size() == partial_aggs.size()) {
        // Pass-through: rename partial aggregate outputs to final aggregate names.
        std::string result = newline(indent);
        result += "(SELECT ";
        for (size_t i = 0; i < group_keys_for_sql.size(); ++i) {
          if (i > 0) result += ", ";
          result += group_keys_for_sql[i].name;
        }
        for (size_t i = 0; i < aggregates_for_sql.size(); ++i) {
          result += ", ";
          // Use the partial's aggregate alias as the input column name
          const auto partial_alias_it = partial_aliases.find(partial_aggs[i]);
          const std::string partial_col = (partial_alias_it != partial_aliases.end())
                                           ? partial_alias_it->second
                                           : partial_aggs[i].name;
          const auto final_alias_it = aliases_for_sql.find(aggregates_for_sql[i]);
          if (final_alias_it != aliases_for_sql.end()) {
            result += partial_col + " AS " + final_alias_it->second;
          } else {
            result += partial_col;
          }
        }
        result += newline(indent);
        result += "FROM " + firstChild()->treeSQL(indent + 1);
        result += newline(indent);
        result += ") AS " + subquerySQLAlias();
        return result;
      }
    }
  }

  std::string result = newline(indent);
  result += "(SELECT ";
  for (size_t i = 0; i < group_keys_for_sql.size(); ++i) {
    if (i > 0) {
      result += ", ";
    }
    result += group_keys_for_sql[i].name;
    if (group_keys_for_sql[i].hasAlias()) {
      result += " AS " + group_keys_for_sql[i].alias;
    }
  }

  auto col_count = group_keys_for_sql.size();
  for (const auto& aggregate : aggregates_for_sql) {
    result += col_count++ > 0 ? ", " : "";
    if (aliases_for_sql.contains(aggregate)) {
      result += aggregate.name + " AS " + aliases_for_sql.at(aggregate);
    } else {
      result += aggregate.name;
    }
  }
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  if (!group_keys_for_sql.empty()) {
    result += "GROUP BY " + join(group_keys_for_sql, ", ");
  }
  result += newline(indent);
  result += ") AS " + subquerySQLAlias();
  return result;
}
}
