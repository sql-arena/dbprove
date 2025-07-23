#pragma once

#include "column.h"
#include "node.h"

namespace sql::explain {
class Sort : public Node {
public:
  explicit Sort()
    : Node(NodeType::SORT) {
  }

  std::vector<Column> columns_sorted;
};
}