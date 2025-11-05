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
  std::string filter_condition;

  explicit Node(const NodeType type)
    : type(type)
    , cost(0) {
  }

  std::string cacheTreeSQL_;
  virtual std::string treeSQLImpl(size_t indent) const;
  static std::string newline(size_t indent);

public:
  const NodeType type;
  double rows_estimated = NAN;
  double rows_actual = NAN;
  double cost;
  std::vector<std::string> columns_input;
  std::vector<std::string> columns_output;

  /**
   * Is there a path to the root of the tree that is purely left deep or must we go via a bushy tree to reach
   * the root of the tree.
   * @return true if the node is left deep.
   */
  bool isLeftDeep() const;
  RowCount rowsEstimated() const;;
  RowCount rowsActual() const;
  void setFilter(const std::string& filter);
  auto filterCondition() const { return filter_condition; }

  /**
   * Generate the SQL needed top query data up until this point in the tree.
   * @return
   */
  std::string treeSQL(size_t indent);

  /**
   * Name of the SubQuery when we generate SQL
   */
  std::string subquerySQLAlias() const;

  /// @brief Return the compact, symbolic representation of the node
  virtual std::string compactSymbolic() const = 0;
  virtual std::string renderMuggle(size_t max_width = 0) const = 0;
  std::string typeName() const;
  void debugPrint() const;
  void debugPrintTree();
};

std::string_view to_string(const NodeType type);
} // namespace sql::explain