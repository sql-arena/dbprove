#include "explain/plan.h"

#include <iostream>

constexpr const char* HASH_BUILD_CHILD = "└";
constexpr const char* VERTICAL_LINE = "│";
constexpr const char* UNION_NOT_LAST = "├";
constexpr const char* HORIZONTAL_LINE = "─";
constexpr const char* UNION_LAST_CHILD = "└";

std::string sql::explain::Plan::render(const RenderMode mode) const {
  std::string result = "\n\n--------------PLAN--------------\n";
  std::string indent = "";

  struct Frame {
    Node* node;
    std::string indent;
  };


  std::vector<Frame> parentSplitNodes;
  for (auto& node : plan_tree->depth_first()) {
    /* Coming back up the tree. If I am the last descendant of a union, I need to have my indentation removed */
    if (parentSplitNodes.size() > 0 && node.depth() < parentSplitNodes.back().node->depth()) {
      auto parent_type = parentSplitNodes.back().node->type;
      if (parent_type == NodeType::UNION) {
        parentSplitNodes.pop_back();
      }
    }

    /* Joins only indent the build side, to keep things compact */
    if (node.parent().type == NodeType::JOIN && node.parent().lastChild() == &node) {
      parentSplitNodes.pop_back();
    }

    /* Render ancestor lines */
    for (size_t i = 0; i < parentSplitNodes.size(); ++i) {
      const auto& ancestor = parentSplitNodes[i].node;
      auto& indent = parentSplitNodes[i].indent;
      if (ancestor->type == NodeType::JOIN && &node == ancestor->firstChild()) {
        result += "│└";
      } else if (ancestor->type == NodeType::UNION && &node == ancestor->lastChild()) {
        result += "└─";
        indent = "  ";  // Last children of UNION must just be indented
      } else if (ancestor->type == NodeType::UNION && &node.parent() == ancestor) {
        result += "├─";
      } else {
        result += indent;
      }
    }

    if (node.children().size() > 1) {
      parentSplitNodes.push_back({&node, "│ "});
    }

    // Finally, the node itself
    switch (mode) {
      case RenderMode::SYMBOLIC:
        result += node.compactSymbolic();
        break;
      case RenderMode::MUGGLE:
        result += node.renderMuggle();
        break;
    }
    result += "\n";
  }
  return result;
}