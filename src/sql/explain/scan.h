#pragma once
#include "explain/node.h"

namespace sql::explain {
class Scan : public Node {

public:
  explicit Scan(const std::string& table_name)
    : Node(NodeType::SCAN)
    , table_name(table_name) {
  }

  std::string compactSymbolic() const override;;

  std::string renderMuggle() const override;

  const std::string table_name;
};
}  // namespace sql::explain