#pragma once
#include "node_type.h"
#include <dbprove/sql/sql.h>
#include <dbprove/common/tree_node.h>
#include <string>
#include <vector>

namespace sql::explain {
/**
 *
 * @brief Plan node in the Explain tree
 *
 * @notes double is used for row estimation.
 * It is possible (and likely frequent) for query planners to estimate
 * something that is bigger than MAX_INT64
 */
class Node : public TreeNode<Node> {
protected:
  explicit Node(const NodeType type)
    : type(type)
    , cost(0) {
  }

public:
  static constexpr double UNKNOWN = -INFINITY;
  const NodeType type;
  double rows_estimated = 0.0;
  double rows_actual = UNKNOWN;
  double cost;
  std::string filter_condition;
  std::vector<std::string> columns_input;
  std::vector<std::string> columns_output;

  RowCount rowsEstimated() const;;
  RowCount rowsActual() const;

  /// @brief Return the compact, symbolic representation of the node
  virtual std::string compactSymbolic() const = 0;
  virtual std::string renderMuggle(size_t max_width = 0) const = 0;
  std::string typeName() const;
  void debugPrint() const;
};
} // namespace sql::explain