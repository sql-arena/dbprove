#include "plan.h"

#include <iostream>

constexpr const char* HASH_BUILD_CHILD = "└";
constexpr const char* VERTICAL_LINE = "│";
constexpr const char* UNION_NOT_LAST = "├";
constexpr const char* HORIZONTAL_LINE = "─";
constexpr const char* UNION_LAST_CHILD = "└";

std::string sql::explain::Plan::render(const RenderMode mode) const {
  std::string result = "\n\n--------------PLAN--------------\n";
  std::string indent = "";
  std::stack<Node*> parentSplitNodes;
  bool last_descendant = false;
  for (auto& node : plan_tree->depth_first()) {
    Node* ancestor_split = nullptr;
    if (parentSplitNodes.size() > 0) {
      ancestor_split = parentSplitNodes.top();
    }

    /* Render ancestor lines */
    for (size_t i = 1; i < parentSplitNodes.size(); ++i) {
      result += VERTICAL_LINE;
      result += " ";
    }

    const auto parent_type = ancestor_split ? ancestor_split->type : NodeType::UNKNOWN;

    switch(parent_type) {
      case NodeType::JOIN: {
        const auto left_child = ancestor_split && ancestor_split->firstChild() == &node;
        const auto right_child = ancestor_split && ancestor_split->lastChild() == &node;
        const auto has_parent_join = ancestor_split != nullptr;

        if (left_child) {
          result += VERTICAL_LINE;
          result += HASH_BUILD_CHILD;
        }
        else if (right_child) {
          // All nodes on the right side of the join move in one indention, including the tope node
          parentSplitNodes.pop();
        }
        else if (has_parent_join) {
          result += VERTICAL_LINE;
          result += " ";
        }
        break;
      }
      case NodeType::UNION: {
        const auto direct_child = ancestor_split && &node.parent() == ancestor_split;
        if (direct_child) {
          last_descendant = ancestor_split->lastChild() == &node;
          result += (last_descendant) ? UNION_LAST_CHILD : UNION_NOT_LAST;
          result += HORIZONTAL_LINE;
        }
        else {
          result += (last_descendant) ? " " : VERTICAL_LINE;
          result += " ";
        }
        break;
      }
      default:
        break;
    }

    switch (mode) {
      case RenderMode::SYMBOLIC:
        result += node.compactSymbolic();
        break;
      case RenderMode::MUGGLE:
        result += node.renderMuggle();
        break;
    }

    result += "\n";
    if (node.type == NodeType::JOIN || node.type == NodeType::UNION) {
      parentSplitNodes.push(&node);
    }
  }
  return result;
}

