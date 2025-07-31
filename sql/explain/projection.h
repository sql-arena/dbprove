#pragma once
#include "node.h"

namespace sql::explain {
class Projection : public Node {
  const constexpr char* symbol_ = "π";
public:
  Projection()
    : Node(NodeType::PROJECTION) {
  }

  std::string compactSymbolic() const override {
    std::string result;
    result+=symbol_;
    result+="{";
    // TODO: add the actual projection here
    result+="}";
    return result;
  }
  std::string renderMuggle() const override {
    std::string result = "PROJECT ";
    return result;
  }
};
}