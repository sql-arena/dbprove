#pragma once
#include "../include/dbprove/sql/explain/node.h"
#include "../include/dbprove/sql/explain/node_type.h"

namespace sql::explain {
class Select : public Node {
  static constexpr const char* symbol_ = "π";
public:
  Select()
    : Node(NodeType::SELECT) {
  }

  std::string compactSymbolic() const override {
    std::string result;
    result+=symbol_;
    result+="{";
    for (auto& column: columns_output) {
      result+=column;
      result+=", ";
    }
    result+="}";
    return result;
  }
  std::string renderMuggle(size_t max_width) const override {
    std::string result = "SELECT";
    return result;
  }
};
}