#pragma once
#include "explain/column.h"
#include "explain/node.h"
#include <string>

namespace sql::explain {
class GroupBy : public Node {
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

  std::string strategyName() const;

  std::string compactSymbolic() const override;

  std::string renderMuggle() const override;
  const Strategy strategy;
  const std::vector<Column> group_keys;
  const std::vector<Column> aggregates;
};
}