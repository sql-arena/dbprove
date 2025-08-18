#include "explain/plan.h"

#include <iostream>
#include <rang.hpp>

#include "cutoff.h"
#include "dbprove/common/pretty.h"

constexpr auto HASH_BUILD_CHILD = "└";
constexpr auto VERTICAL_LINE = "│";
constexpr auto UNION_NOT_LAST = "├";
constexpr auto HORIZONTAL_LINE = "─";
constexpr auto UNION_LAST_CHILD = "└";

namespace sql::explain {
RowCount countRowsByNode(Node& node, const NodeType type) {
  double result = 0;
  for (const auto& n : node.depth_first()) {
    if (n.type == type) {
      result += n.rows_actual;
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsAggregated() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::GROUP_BY) {
      // The input of an aggregate is the amount of rows added to the aggregate
      result += n.firstChild()->rows_actual;
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsJoined() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::JOIN) {
      // The probe side of the join is the number of joined rows, unless we created more by cardinality increase
      result += std::max(n.firstChild()->rows_actual, n.rows_actual);
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsSorted() const {
  return countRowsByNode(planTree(), NodeType::SORT);
}

RowCount Plan::rowsProcessed() const {
  return rowsAggregated() + rowsJoined() + rowsSorted();
}

RowCount Plan::rowsScanned() const {
  return countRowsByNode(planTree(), NodeType::SCAN);
}

RowCount Plan::rowsFiltered() const {
  double result = 0;
  for (const auto& node : planTree().depth_first()) {
    if (node.childCount() == 0) {
      continue;
    }
    const auto output_rows = node.rows_actual;
    const auto input_rows = node.firstChild()->rows_actual;
    result += input_rows - output_rows;
  }
  return cutoff(result);
}


void Plan::render(std::ostream& out, size_t max_width, RenderMode mode) const {
  std::string indent = "";
  std::string divider = "  ";
  struct Frame {
    Node* node;
    std::string indent;
  };

  out << rang::style::bold;
  out << "Estimate" << divider;
  out << "  Actual" << divider;
  out << "Operator";
  out << rang::style::reset << std::endl;
  // Remaining length after we have rendered actual/esimate and dividers
  const auto node_width = max_width - 2 * 10 - divider.size();

  std::vector<Frame> parent_split_nodes;
  for (auto& node : plan_tree->depth_first()) {
    /* Estimates/actuals */
    out << dbprove::common::PrettyHumanCount(node.rowsEstimated()) << divider;
    out << dbprove::common::PrettyHumanCount(node.rowsActual()) << divider;

    /* Coming back up the tree. If I am the last descendant of a union, I need to have my indentation removed */
    if (!parent_split_nodes.empty() && node.depth() < parent_split_nodes.back().node->depth()) {
      const auto parent_type = parent_split_nodes.back().node->type;
      if (parent_type == NodeType::UNION) {
        parent_split_nodes.pop_back();
      }
    }

    /* Joins only indent the build side, to keep things compact */
    if (node.parent().type == NodeType::JOIN && node.parent().lastChild() == &node) {
      parent_split_nodes.pop_back();
    }

    /* Render ancestor lines */
    for (size_t i = 0; i < parent_split_nodes.size(); ++i) {
      const auto& ancestor = parent_split_nodes[i].node;
      auto& indent = parent_split_nodes[i].indent;
      if (ancestor->type == NodeType::JOIN && &node == ancestor->firstChild()) {
        out << "│└";
      } else if (ancestor->type == NodeType::UNION && &node == ancestor->lastChild()) {
        out << "└─";
        indent = "  "; // Last children of UNION must just be indented
      } else if (ancestor->type == NodeType::UNION && &node.parent() == ancestor) {
        out << "├─";
      } else {
        out << indent;
      }
    }

    if (node.children().size() > 1) {
      parent_split_nodes.push_back({&node, "│ "});
    }

    // Finally, the node itself
    switch (mode) {
      case RenderMode::SYMBOLIC:
        out << node.compactSymbolic();
        break;
      case RenderMode::MUGGLE:
        out << node.renderMuggle(node_width);
        break;
    }
    out << std::endl;
  }
}
}