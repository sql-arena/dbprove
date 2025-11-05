#include <dbprove/sql/explain/node.h>
#include <iostream>
#include "cutoff.h"


namespace sql::explain {
std::string_view to_string(const NodeType type) {
  return magic_enum::enum_name(type);
}

std::string Node::typeName() const {
  switch (type) {
    case NodeType::JOIN:
      return "JOIN";
    case NodeType::GROUP_BY:
      return "GROUP BY";
    case NodeType::SORT:
      return "SORT";
    case NodeType::SELECT:
      return "SELECT";
    case NodeType::SCAN:
    case NodeType::SCAN_EMPTY:
    case NodeType::SCAN_MATERIALISED:
      return "SCAN";
    case NodeType::PROJECTION:
      return "PROJECTION";
    case NodeType::DISTRIBUTE:
      return "DISTRIBUTE";
    case NodeType::LIMIT:
      return "LIMIT";
    case NodeType::UNION:
      return "UNION";
    case NodeType::FILTER:
      return "FILTER";
    case NodeType::SEQUENCE:
      return "SEQUENCE";
    default:
      return "UNKNOWN";
  }
}

void Node::debugPrint() const {
  std::cout << "type: " << typeName() << " depth: " << std::to_string(depth()) << std::endl;
}

void Node::debugPrintTree() {
  for (const auto& n : depth_first()) {
    for (size_t i = 0; i < n.depth(); ++i) {
      std::cout << "  ";
    }
    n.debugPrint();
  }
}


std::string Node::treeSQLImpl(const size_t indent) const {
  return firstChild()->treeSQL(indent);
}

std::string Node::newline(const size_t indent) {
  std::string result = "\n";
  for (size_t i = 0; i < indent; ++i) {
    result += "  ";
  }
  return result;
}

bool Node::isLeftDeep() const {
  auto n = this;
  size_t rightParents = 0;
  while (!n->isRoot()) {
    const auto parent = &n->parent();
    if (parent->firstChild() == n && parent->childCount() > 1) {
      rightParents++;
    }
    n = parent;
  }
  return rightParents <= 1;
}

RowCount Node::rowsEstimated() const {
  return cutoff(rows_estimated);
}

RowCount Node::rowsActual() const {
  return cutoff(rows_actual);
}

void Node::setFilter(const std::string& filter) {
  filter_condition = cleanExpression(filter);
}

std::string Node::treeSQL(const size_t indent) {
  if (cacheTreeSQL_.empty()) {
    cacheTreeSQL_ = treeSQLImpl(indent);
  }
  return cacheTreeSQL_;
}

std::string Node::subquerySQLAlias() const {
  return std::string(to_string(type)) + "_" + std::to_string(id());
}
}