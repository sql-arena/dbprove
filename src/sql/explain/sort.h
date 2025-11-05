#pragma once

#include "explain/column.h"
#include "explain/node.h"


namespace sql::explain {
class Sort final : public Node {
public:
  explicit Sort(const std::vector<Column>& columns_sorted)
    : Node(NodeType::SORT)
    , columns_sorted(columns_sorted) {
  }

  std::string compactSymbolic() const override;

  std::string renderMuggle(size_t max_width) const override;

  std::vector<Column> columns_sorted;
};
}
