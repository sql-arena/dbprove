#pragma once
#include <utility>

#include "node.h"
#include "common/string.h"

namespace sql::explain {
class Scan : public Node {
  static constexpr const char* symbol_ = "📄";
public:
  explicit Scan(const std::string& table_name)
    : Node(NodeType::SCAN)
    , table_name(table_name) {
  }

  std::string compactSymbolic() const override {
    return table_name;
  };

  std::string renderMuggle() const override {
    std::string result = "SCAN " + table_name;
    return result;
  }

  const std::string table_name;
};
}  // namespace sql::explain