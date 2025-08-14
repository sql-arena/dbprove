#include "../include/dbprove/sql/explain/node.h"

#include <iostream>


std::string sql::explain::Node::typeName() const {
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
      return "SCAN";
    case NodeType::PROJECTION:
      return "PROJECTION";
    case NodeType::DISTRIBUTE:
      return "DISTRIBUTE";
    case NodeType::LIMIT:
      return "LIMIT";
    default:
      return "UNKNOWN";
  }
}

void sql::explain::Node::debugPrint() const {
  std::cout << "type: " << typeName() << " depth: " << std::to_string(depth()) << std::endl;
}