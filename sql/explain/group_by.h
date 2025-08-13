#pragma once
#include "column.h"
#include "node.h"
#include <dbprove/common/string.h>

namespace sql::explain {
class GroupBy : public Node {
  static constexpr const char* symbol_ = "γ";

public:
  enum class Strategy {
    HASH,
    SORT_MERGE,
    PARTIAL,
    SIMPLE
  };

  explicit GroupBy(const Strategy strategy,
                   const std::vector<Column>& group_keys,
                   const std::vector<Column>& aggregates
      )
    : Node(NodeType::GROUP_BY)
    , strategy(strategy)
    , group_keys(group_keys)
    , aggregates(aggregates) {
  }

  std::string strategyName() const {
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
  std::string compactSymbolic() const override {
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
  std::string renderMuggle() const override {
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
  const Strategy strategy;
  const std::vector<Column> group_keys;
  const std::vector<Column> aggregates;
};
}