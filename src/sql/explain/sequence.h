#pragma once
#include "explain/node.h"

namespace sql::explain {
/**
 * A container node for running two things in sequence.
 */
class Sequence final : public Node {
public:
  explicit Sequence()
    : Node(NodeType::SEQUENCE) {
  }

  std::string compactSymbolic() const override {
    return ";";
  }

  std::string renderMuggle(size_t max_width) const override {
    return "SEQUENCE";
  }
};
}
