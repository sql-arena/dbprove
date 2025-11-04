#pragma once
#include "explain/column.h"
#include "explain/node.h"

namespace sql::explain {
class Column;

class Projection : public Node {
  static const constexpr char* symbol_ = "π";

public:
  Projection(const std::vector<Column>& columns_projected)
    : Node(NodeType::PROJECTION)
    , columns_projected(columns_projected) {
  }

  std::string compactSymbolic() const override;

  std::string renderMuggle(size_t max_width) const override;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  std::vector<Column> columns_projected;
};
}
