#include "explain/plan.h"
#include "cutoff.h"
#include "dbprove/common/pretty.h"

#include <cmath>
#include <iostream>
#include <ranges>
#include <rang.hpp>

#include "join.h"
#include "sql_exceptions.h"


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

std::string order_to_string(const int8_t magnitude) {
  return std::to_string(1 << std::abs(magnitude)) + "x";
}

std::string Magnitude::to_string() const {
  std::string magnitude;
  if (value == 0) {
    magnitude = "=";
  } else if (value == Plan::MisEstimation::INFINITE_OVER) {
    magnitude += ">";
    magnitude += order_to_string(value);
  } else if (value == Plan::MisEstimation::INFINITE_UNDER) {
    magnitude += "<";
    magnitude += order_to_string(value);
  } else {
    magnitude = value < 0 ? "-" : "+";
    magnitude += order_to_string(value);
  }
  return magnitude;
}

bool Plan::canEstimate() const {
  for (const auto& n : planTree().depth_first()) {
    if (std::isnan(n.rows_estimated)) {
      return false;
    }
  }
  return true;
}

RowCount Plan::rowsAggregated() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::GROUP_BY) {
      // The input of an aggregate is the amount of rows added to the aggregate.
      if (n.childCount() > 0) {
        result += n.firstChild()->rows_actual;
      }
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsJoined() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::JOIN) {
      if (n.childCount() < 2) {
        throw ExplainException("Join nodes must have 2 children. The plan parsing must have failed");
      }
      // The probe side of the join is the number of joined rows, unless we created more by cardinality increase
      result += std::max(n.lastChild()->rows_actual, n.rows_actual);
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsHashBuild() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::JOIN) {
      if (n.childCount() < 2) {
        throw ExplainException("Join nodes must have 2 children. The plan parsing must have failed");
      }
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
  return rowsAggregated() + rowsJoined() + rowsSorted() + rowsHashBuild();
}

RowCount Plan::rowsReturned() const {
  return 0; // TODO
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

void Plan::flipJoins() const {
  std::vector<Node*> join_nodes;
  for (auto& node : planTree().depth_first()) {
    if (node.type == NodeType::JOIN) {
      join_nodes.push_back(&node);
    }
  }

  for (const auto node : join_nodes) {
    node->reverseChildren();
  }
}

int8_t estimateOrderOfMagnitude(double estimate, double actual) {
  if (std::isnan(estimate) || std::isnan(actual)) {
    return Plan::MisEstimation::UNKNOWN;
  }
  actual = std::max(actual, 1.0);
  estimate = std::max(estimate, 1.0);
  const auto error = (estimate > actual) ? estimate / actual : actual / estimate;
  auto magnitude = static_cast<int8_t>(std::log2(error));
  magnitude = (estimate > actual) ? magnitude : -magnitude;
  magnitude = std::min(magnitude, Plan::MisEstimation::INFINITE_OVER);
  magnitude = std::max(magnitude, Plan::MisEstimation::INFINITE_UNDER);
  return magnitude;
}

std::vector<Plan::MisEstimation> Plan::misEstimations() const {
  /* Construct mis estimation map, All combination must exist for easy rendering */
  std::map<Operation, std::map<int8_t, MisEstimation>> mis_estimation;
  for (auto& op : {Operation::Join,
                   Operation::Aggregate,
                   Operation::Sort,
                   Operation::Filter,
                   Operation::Scan,
                   Operation::Hash}) {
    mis_estimation.insert({op, {}}).first;
    for (int8_t magnitude = MisEstimation::INFINITE_UNDER; magnitude <= MisEstimation::INFINITE_OVER; magnitude++) {
      mis_estimation[op].emplace(magnitude, MisEstimation{op, magnitude, 0});
    }
  }

  for (const auto& n : planTree().depth_first()) {
    auto magnitude = estimateOrderOfMagnitude(n.rows_estimated, n.rows_actual);
    if (magnitude == Plan::MisEstimation::UNKNOWN) {
      // This engine doesn't support estimation, or we couldn't calculate it.
      continue;
    }
    switch (n.type) {
      case NodeType::JOIN: {
        mis_estimation[Operation::Join][magnitude].count++;
        const auto join_node = reinterpret_cast<const Join*>(&n);
        if (join_node->strategy == Join::Strategy::HASH) {
          const Node& h = join_node->buildChild();
          auto hash_magnitude = estimateOrderOfMagnitude(h.rows_estimated, h.rows_actual);
          mis_estimation[Operation::Hash][hash_magnitude].count++;
          break;
        }
        break;
      }
      case NodeType::SORT:
        mis_estimation[Operation::Sort][magnitude].count++;
        break;
      case NodeType::GROUP_BY:
        mis_estimation[Operation::Aggregate][magnitude].count++;
      case NodeType::SCAN:
        mis_estimation[Operation::Scan][magnitude].count++;
        break;
      case NodeType::FILTER:
        mis_estimation[Operation::Filter][magnitude].count++;
        break;
      default:
        break;
    }
  }
  std::vector<MisEstimation> sorted_mis_estimations;
  for (const auto& inner_map : mis_estimation | std::views::values) {
    for (const auto& e : inner_map | std::views::values) {
      sorted_mis_estimations.push_back(e);
    }
  }
  std::sort(sorted_mis_estimations.begin(), sorted_mis_estimations.end());

  return sorted_mis_estimations;
}


void Plan::render(std::ostream& out, size_t max_width, RenderMode mode) const {
  std::string indent;
  using namespace dbprove::common;
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
  std::vector<Frame> parent_split_nodes;
  for (auto& node : plan_tree->depth_first()) {
    const auto used_before = out.tellp();
    /* Estimates/actuals */
    std::string estimate = std::isnan(node.rows_estimated) ? PrettyUnknown() : PrettyHumanCount(node.rowsEstimated());
    out << estimate << divider;
    out << PrettyHumanCount(node.rowsActual()) << divider;

    /* Coming back up the tree. If I am the last descendant of a union, I need to have my indentation removed.*/
    if (!parent_split_nodes.empty() && node.depth() < parent_split_nodes.back().node->depth()) {
      const auto parent_type = parent_split_nodes.back().node->type;
      if (parent_type == NodeType::UNION || parent_type == NodeType::SEQUENCE) {
        parent_split_nodes.pop_back();
      }
    }

    /* Joins only indent the build side. Keep things compact.*/
    if (node.parent().type == NodeType::JOIN && node.parent().lastChild() == &node) {
      parent_split_nodes.pop_back();
    }

    /* Render ancestor lines */
    for (size_t i = 0; i < parent_split_nodes.size(); ++i) {
      const auto& ancestor = parent_split_nodes[i].node;
      auto& current_indent = parent_split_nodes[i].indent;
      if (ancestor->type == NodeType::JOIN && &node == ancestor->firstChild()) {
        out << "│└";
      } else if ((ancestor->type == NodeType::UNION || ancestor->type == NodeType::SEQUENCE) && &node == ancestor->
                 lastChild()) {
        out << "└─";
        current_indent = "  "; // Last children of UNION must just be indented
      } else if ((ancestor->type == NodeType::UNION || ancestor->type == NodeType::SEQUENCE) && &node.parent() ==
                 ancestor) {
        out << "├─";
      } else {
        out << current_indent;
      }
    }

    if (node.children().size() > 1) {
      parent_split_nodes.push_back({&node, "│ "});
    }

    // Remaining length in the terminal
    const auto node_width = max_width - (out.tellp() - used_before);

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