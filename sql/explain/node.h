#pragma once
#include "node_type.h"
#include "sql_type.h"
#include "common/tree_node.h"

namespace sql::explain {
class Node : TreeNode {
protected:
  explicit Node(const NodeType type)
    : type(type) {
  }

public:
  const NodeType type;
  RowCount rows_estimated = 0;
  RowCount rows_actual = 0;
  std::vector<std::string> columns_input;
  std::vector<std::string> columns_output;
};
}  // namespace sql::explain