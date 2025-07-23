#pragma once
#include "node.h"

namespace sql::explain {
class Expression : public Node {
public:
  Expression()
    : Node(NodeType::EXPRESSION) {
  }
};
}