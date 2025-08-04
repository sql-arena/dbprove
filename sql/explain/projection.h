#pragma once
#include "column.h"
#include "node.h"

namespace sql::explain {
class Column;

class Projection : public Node {
  static const constexpr char* symbol_ = "π";
public:
  Projection(const std::vector<Column>& columns_projected)
    : Node(NodeType::PROJECTION), columns_projected(columns_projected) {
  }

  std::string compactSymbolic() const override {
    std::string result;
    result+=symbol_;
    result+="{";
    result+= Column::join(columns_projected, ", ");
    result+="}";
    return result;
  }
  std::string renderMuggle() const override {
    std::string result = "PROJECT ";
    result+= "(";
    result+= Column::join(columns_projected, ", ");
    result+= ")";
    return result;
  }
  std::vector<Column> columns_projected;
};
}