#pragma once

#include "explain/node.h"

namespace sql::explain {
/**
* Engines may optimise away entire scans. This node serves as a left node to hold that info
*/
class ScanEmpty : public Node {
public:
  explicit ScanEmpty()
    : Node(NodeType::SCAN_EMPTY) {
  }

  std::string compactSymbolic() const override;;

  std::string renderMuggle() const override;
};
}  // namespace sql::explain