#pragma once
#include <utility>

#include "node.h"
#include <dbprove/common/string.h>

namespace sql::explain {
/**
* Engines may optimise away entire scans. This node serves as a left node to hold that info
*/
class ScanEmpty : public Node {
  static constexpr const char* symbol_ = "📄";
public:
  explicit ScanEmpty()
    : Node(NodeType::SCAN_EMPTY) {
  }

  std::string compactSymbolic() const override {
    return std::string(symbol_);
  };

  std::string renderMuggle() const override {
    return "SCAN EMPTY";
  }
};
}  // namespace sql::explain