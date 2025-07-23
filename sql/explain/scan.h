#pragma once
#include <utility>

#include "node.h"

namespace sql::explain {
class Scan : Node {
public:
  explicit Scan(std::string  table_name)
    : Node(NodeType::SCAN)
    , table_name(std::move(table_name)) {
  }

  std::string table_name;
};
}  // namespace sql::explain