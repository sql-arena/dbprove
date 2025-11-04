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
    SIMPLE,
    UNKNOWN
  };

  explicit GroupBy(const Strategy strategy, const std::vector<Column>& group_keys,
                   const std::vector<Column>& aggregates)
    : Node(NodeType::GROUP_BY)
    , strategy(strategy)
    , group_keys(group_keys)
    , aggregates(aggregates) {
  }

  std::string strategyName() const;

  std::string compactSymbolic() const override;

  std::string renderMuggle(size_t max_width) const override;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  const Strategy strategy;
  const std::vector<Column> group_keys;
  const std::vector<Column> aggregates;
};
}