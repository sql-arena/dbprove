#pragma once

#include "explain/column.h"
#include "explain/node.h"
#include "sql_exceptions.h"

namespace sql::explain {
class Sort : public Node {


public:
  explicit Sort(const std::vector<Column>& columns_sorted)
    : Node(NodeType::SORT), columns_sorted(columns_sorted) {
  }

  std::string compactSymbolic() const override;

  std::string renderMuggle() const override;


  std::vector<Column> columns_sorted;
};
}