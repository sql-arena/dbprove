#pragma once

#include "column.h"
#include "node.h"
#include "node_type.h"

namespace sql::explain {
class Limit : public Node {
  static const constexpr char* symbol= "λ";
public:
  explicit Limit(RowCount limit_count)
    : Node(NodeType::LIMIT), limit_count(limit_count) {
  }

  std::string compactSymbolic() const override {
    std::string result;
    result+=symbol;
    result+="{" + std::to_string(limit_count) + "}";
    return result;
  };

  std::string renderMuggle() const override {
    std::string result = "LIMIT ";
    result+= std::to_string(limit_count);
    return result;

  }

  const RowCount limit_count;

};
}