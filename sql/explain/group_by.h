#pragma once
#include "node.h"

namespace sql::explain {
class GroupBy : public Node {
public:
  explicit GroupBy()
    : Node(NodeType::GROUP_BY) {
  }
};
}