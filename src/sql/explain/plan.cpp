#include "explain/plan.h"
#include "cutoff.h"
#include "dbprove/common/pretty.h"

#include <iostream>
#include <ranges>
#include <rang.hpp>

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

int8_t estimateOrderOfMagnitude(double estimate, double actual) {
  actual = std::max(actual, 1.0);
  estimate = std::max(estimate, 1.0);
  const auto error = (estimate > actual) ? estimate / actual : -actual / estimate;
  auto magnitude = static_cast<int8_t>(std::log2(error));
  magnitude = std::min(magnitude, Plan::MisEstimation::INFINITE_OVER);
  magnitude = std::max(magnitude, Plan::MisEstimation::INFINITE_UNDER);
  return magnitude;
}

std::vector<Plan::MisEstimation> Plan::misEstimations() const {
  /* Construct mis estimation map, All combination must exist for easy rendering */
  std::map<Operation, std::map<int8_t, MisEstimation>> mis_estimation;
  for (auto& op : {Operation::JOIN, Operation::SORT, Operation::FILTER, Operation::AGGREGATE}) {
    mis_estimation.insert({op, {}}).first;
    for (int8_t magnitude = MisEstimation::INFINITE_UNDER;
         magnitude <= MisEstimation::INFINITE_OVER;
         magnitude++) {
      mis_estimation[op].emplace(magnitude, MisEstimation{op, magnitude, 0});
    };
  }

  for (const auto& n : planTree().depth_first()) {
    auto magnitude = estimateOrderOfMagnitude(n.rows_estimated, n.rows_actual);
    switch (n.type) {
      case NodeType::JOIN:
        mis_estimation[Operation::JOIN][magnitude].count++;
        break;
      case NodeType::SORT:
        mis_estimation[Operation::SORT][magnitude].count++;
        break;
      case NodeType::GROUP_BY:
        mis_estimation[Operation::AGGREGATE][magnitude].count++;
        break;
      default:
        break;
    }
  }
  std::vector<MisEstimation> sorted_mis_estimations;
  for (const auto& inner_map : mis_estimation | std::views::values) {
    for (const auto& mis_estimation : inner_map | std::views::values) {
      sorted_mis_estimations.push_back(mis_estimation);
    }
  }
  std::sort(sorted_mis_estimations.begin(), sorted_mis_estimations.end());

  return sorted_mis_estimations;
}


void Plan::render(std::ostream& out, size_t max_width, RenderMode mode) const {
  std::string indent;
  const std::string divider = "  ";
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