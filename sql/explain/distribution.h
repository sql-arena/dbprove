#pragma once
#include "node.h"
#include "node_type.h"

namespace sql::explain {
class Column;

class Distribute : public Node {
public:
  explicit Distribute()
    : Node(NodeType::DISTRIBUTE) {
  }

  std::vector<Column> columns_distribute;
};
}  // namespace sql::explain
