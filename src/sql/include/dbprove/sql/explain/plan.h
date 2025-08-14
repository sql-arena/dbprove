#pragma once
#include "node.h"
#include <memory>


namespace sql::explain {
class Node;
class Plan {
  std::unique_ptr<Node> plan_tree;

public:
  enum class RenderMode {
    SYMBOLIC,
    MUGGLE
  };
  explicit Plan(std::unique_ptr<Node> root_node)
    : plan_tree(std::move(root_node)) {
  }

  double planning_time = 0.0;
  double execution_time = 0.0;

  Node& planTree() const {
    return *plan_tree;
  }

  /// @brief Render the plan as symbolic, relational algebra like, representation
  std::string render(RenderMode mode = RenderMode::MUGGLE) const;;

};
}