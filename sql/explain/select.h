#pragma once
#include "node.h"
#include "node_type.h"

namespace sql::explain {
class Select : public Node {
public:
  Select()
    : Node(NodeType::SELECT) {
  }
};
}