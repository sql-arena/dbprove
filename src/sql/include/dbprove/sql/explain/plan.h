#pragma once
#include "node.h"
#include <memory>
#include <ostream>


namespace sql::explain
{
  class Node;

  class Plan
  {
    std::unique_ptr<Node> plan_tree;

  public:
    enum class RenderMode
    {
      SYMBOLIC,
      MUGGLE
    };

    explicit Plan(std::unique_ptr<Node> root_node)
      : plan_tree(std::move(root_node))
    {
    }

    double planning_time = 0.0;
    double execution_time = 0.0;

    [[nodiscard]] Node& planTree() const
    {
      return *plan_tree;
    }

    [[nodiscard]] RowCount rowsAggregated() const;
    [[nodiscard]] RowCount rowsSorted() const;
    [[nodiscard]] RowCount rowsJoined() const;
    [[nodiscard]] RowCount rowsProcessed() const;
    [[nodiscard]] RowCount rowsReturned() const;
    [[nodiscard]] RowCount rowsScanned() const;
    [[nodiscard]] RowCount rowsFiltered() const;

    /**
     * Render the plan to human-readable format
     * @param mode Mode to use for rendering
     */
    void render(std::ostream& out, size_t max_width, RenderMode mode = RenderMode::MUGGLE) const;;
  };
}
