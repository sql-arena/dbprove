#include "explain/plan.h"
#include "cutoff.h"
#include "dbprove/common/pretty.h"
#include <dbprove/sql/connection_base.h>
#include <plog/Log.h>

#include <cmath>
#include <iostream>
#include <ranges>
#include <rang.hpp>

#include "join.h"
#include "scan.h"
#include "sql_exceptions.h"


constexpr auto HASH_BUILD_CHILD = "└";
constexpr auto VERTICAL_LINE = "│";
constexpr auto UNION_NOT_LAST = "├";
constexpr auto HORIZONTAL_LINE = "─";
constexpr auto UNION_LAST_CHILD = "└";

namespace sql::explain {
void Plan::syncSequenceRowCounts(Node& root) {
  for (auto& node : root.depth_first()) {
    if (node.type != NodeType::SEQUENCE || node.childCount() == 0 || node.lastChild() == nullptr) {
      continue;
    }
    node.rows_estimated = node.lastChild()->rows_estimated;
    node.rows_actual = node.lastChild()->rows_actual;
  }
}

RowCount countRowsByNode(Node& node, const NodeType type) {
  double result = 0;
  for (const auto& n : node.depth_first()) {
    if (n.type == type) {
      if (std::isnan(n.rows_actual) || std::isinf(n.rows_actual)) {
        return ROWS_UNKNOWN;
      }
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
        double rows = n.firstChild()->rows_actual;
        if (std::isnan(rows) || std::isinf(rows)) {
          return ROWS_UNKNOWN;
        }
        result += rows;
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
      double probe_rows = n.lastChild()->rows_actual;
      double actual_rows = n.rows_actual;
      if (std::isnan(probe_rows) || std::isinf(probe_rows) || std::isnan(actual_rows) || std::isinf(actual_rows)) {
        return ROWS_UNKNOWN;
      }
      result += std::max(probe_rows, actual_rows);
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
      double build_rows = n.firstChild()->rows_actual;
      double actual_rows = n.rows_actual;
      if (std::isnan(build_rows) || std::isinf(build_rows) || std::isnan(actual_rows) || std::isinf(actual_rows)) {
        return ROWS_UNKNOWN;
      }
      result += std::max(build_rows, actual_rows);
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
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type == NodeType::SCAN_MATERIALISED) {
      if (std::isnan(n.rows_actual) || std::isinf(n.rows_actual)) {
        return ROWS_UNKNOWN;
      }
      result += n.rows_actual;
      continue;
    }
    if (n.type != NodeType::SCAN) {
      continue;
    }
    const auto& scan = reinterpret_cast<const Scan&>(n);
    if (scan.strategy == Scan::Strategy::SCAN) {
      if (std::isnan(n.rows_actual) || std::isinf(n.rows_actual)) {
        return ROWS_UNKNOWN;
      }
      result += n.rows_actual;
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsSeeked() const {
  double result = 0;
  for (const auto& n : planTree().depth_first()) {
    if (n.type != NodeType::SCAN) {
      continue;
    }
    const auto& scan = reinterpret_cast<const Scan&>(n);
    if (scan.strategy == Scan::Strategy::SEEK) {
      if (std::isnan(n.rows_actual) || std::isinf(n.rows_actual)) {
        return ROWS_UNKNOWN;
      }
      result += n.rows_actual;
    }
  }
  return cutoff(result);
}

RowCount Plan::rowsDistributed() const {
  return countRowsByNode(planTree(), NodeType::DISTRIBUTE);
}

RowCount Plan::rowsFiltered() const {
  double result = 0;
  for (const auto& node : planTree().depth_first()) {
    if (node.childCount() == 0) {
      continue;
    }
    const auto output_rows = node.rows_actual;
    const auto input_rows = node.firstChild()->rows_actual;
    if (std::isnan(output_rows) || std::isinf(output_rows) || std::isnan(input_rows) || std::isinf(input_rows)) {
      return ROWS_UNKNOWN;
    }
    result += std::max(0.0, input_rows - output_rows);
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
                   Operation::Hash,
                   Operation::Distribute}) {
    (void)mis_estimation.insert({op, {}}).first;
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
      case NodeType::DISTRIBUTE:
        mis_estimation[Operation::Distribute][magnitude].count++;
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
  struct SnapshotNode {
    Node* node;
    Node* parent;
    Node* first_child;
    Node* last_child;
    size_t child_count;
    size_t depth;
  };
  struct Frame {
    SnapshotNode snapshot;
    std::string indent;
  };
  auto snapshot_children = [](Node& node) {
    std::vector<Node*> snapshot;
    snapshot.reserve(node.childCount());
    for (auto* child : node.children()) {
      if (child != nullptr) {
        snapshot.push_back(child);
      }
    }
    return snapshot;
  };
  auto snapshot_depth_first = [&](Node& root) {
    std::vector<SnapshotNode> snapshot;
    std::stack<Node*> stack;
    stack.push(&root);
    while (!stack.empty()) {
      Node* current = stack.top();
      stack.pop();
      const auto children = snapshot_children(*current);
      snapshot.push_back(SnapshotNode{
          .node = current,
          .parent = current->isRoot() ? current : &current->parent(),
          .first_child = children.empty() ? nullptr : children.front(),
          .last_child = children.empty() ? nullptr : children.back(),
          .child_count = children.size(),
          .depth = current->depth(),
      });
      for (auto it = children.rbegin(); it != children.rend(); ++it) {
        stack.push(*it);
      }
    }
    return snapshot;
  };
  out << rang::style::bold;
  out << "Estimate" << divider;
  out << "  Actual" << divider;
  out << "Operator";
  out << rang::style::reset << std::endl;
  std::vector<Frame> parent_split_nodes;
  for (const auto& current : snapshot_depth_first(*plan_tree)) {
    auto& node = *current.node;
    const auto used_before = out.tellp();
    /* Estimates/actuals */
    std::string estimate = std::isnan(node.rows_estimated) ? PrettyUnknown() : PrettyHumanCount(node.rowsEstimated());
    out << estimate << divider;
    out << PrettyHumanCount(node.rowsActual()) << divider;

    /* Coming back up the tree. If I am the last descendant of a union, I need to have my indentation removed.*/
    while (!parent_split_nodes.empty() && current.depth < parent_split_nodes.back().snapshot.depth) {
      const auto parent_type = parent_split_nodes.back().snapshot.node->type;
      if (parent_type == NodeType::UNION || parent_type == NodeType::SEQUENCE) {
        parent_split_nodes.pop_back();
      } else {
        break;
      }
    }

    /* Joins only indent the build side. Keep things compact.*/
    if (!parent_split_nodes.empty() && current.parent->type == NodeType::JOIN && current.parent->lastChild() == &node) {
      parent_split_nodes.pop_back();
    }

    /* Render ancestor lines */
    for (size_t i = 0; i < parent_split_nodes.size(); ++i) {
      const auto& ancestor = parent_split_nodes[i].snapshot;
      auto& current_indent = parent_split_nodes[i].indent;
      if (ancestor.node->type == NodeType::JOIN && &node == ancestor.first_child) {
        out << "│└";
      } else if ((ancestor.node->type == NodeType::UNION || ancestor.node->type == NodeType::SEQUENCE)
                 && &node == ancestor.last_child) {
        out << "└─";
        current_indent = "  "; // Last children of UNION must just be indented
      } else if ((ancestor.node->type == NodeType::UNION || ancestor.node->type == NodeType::SEQUENCE)
                 && current.parent == ancestor.node) {
        out << "├─";
      } else {
        out << current_indent;
      }
    }

    if (current.child_count > 1) {
      parent_split_nodes.push_back({current, "│ "});
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

void Plan::fixActuals(sql::ConnectionBase& connection) {
  for (auto& node : planTree().depth_first()) {
    const auto sql = connection.transformActualsSQL(node.actualsSql());
    try {
      node.rows_actual = static_cast<RowCount>(connection.fetchScalar(sql).asInt8());
    } catch (const std::exception& e) {
      PLOGE << "fixActuals failed for node id=" << node.id()
            << " type=" << node.typeName()
            << " error=" << e.what()
            << "\nSQL:\n"
            << sql;
      continue;
    } catch (...) {
      PLOGE << "fixActuals failed for node id=" << node.id()
            << " type=" << node.typeName()
            << " error=<unknown>"
            << "\nSQL:\n"
            << sql;
      continue;
    }
  }
  syncSequenceRowCounts(planTree());
}
}
