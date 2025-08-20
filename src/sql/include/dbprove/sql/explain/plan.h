#pragma once
#include "node.h"
#include <memory>
#include <ostream>


namespace sql::explain
{
  /**
   * Represents the high level operations that roughly map to node types
   */
  enum class Operation
  {
    JOIN = 0, AGGREGATE = 1, SORT = 2, SCAN = 3, FILTER = 4,
  };

  class Node;


  class Plan
  {
    std::unique_ptr<Node> plan_tree;

  public:
    struct MisEstimation
    {
      static constexpr int8_t INFINITE_OVER = 8;
      static constexpr int8_t INFINITE_UNDER = -8;
      Operation operation;
      int8_t order_of_magnitude;
      size_t count;

      bool operator<(const MisEstimation& other) const
      {
        if (order_of_magnitude != other.order_of_magnitude) {
          return order_of_magnitude < other.order_of_magnitude;
        }
        return operation < other.operation;
      }
    };

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
    [[nodiscard]] std::vector<MisEstimation> misEstimations() const;

    /**
     * Render the plan to human-readable format
     * @param mode Mode to use for rendering
     */
    void render(std::ostream& out, size_t max_width, RenderMode mode = RenderMode::MUGGLE) const;;
  };
}
