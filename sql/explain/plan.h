#pragma once
#include <memory>
#include "node.h"



namespace sql::explain {
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


  /// @brief Render the plan as symbolic, relational algebra like, representation
  std::string render(RenderMode mode = RenderMode::MUGGLE) const;;

};
}