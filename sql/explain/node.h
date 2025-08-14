#pragma once
#include "node_type.h"
#include "sql_type.h"

#include <dbprove/common/tree_node.h>

namespace sql::explain {
/**
 *
 * @brief a plan node in the Explain tree
 *
 * @notes double is used for row estimation. It is possible (and likely in many cases) for query planners to estimate
 * something that is bigger than MAX_INT64
 */
class Node : public TreeNode<Node> {
protected:
  explicit Node(const NodeType type)
    : type(type) {
  }

public:
  const NodeType type;
  double rows_estimated = 0.0;
  double rows_actual = 0.0;
  double cost;
  std::string filter_condition;
  std::vector<std::string> columns_input;
  std::vector<std::string> columns_output;
  /// @brief Return the compact, symbolic representation of the node
  virtual std::string compactSymbolic() const = 0;
  virtual std::string renderMuggle() const = 0;
  std::string typeName() const;
  void debugPrint() const;
};
}  // namespace sql::explain