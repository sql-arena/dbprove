#pragma once
#include "explain/node.h"
#include <string>

namespace sql::explain
{
  /**
   * A scan of a materialised result set from another part of the query.
   */
  class ScanMaterialised final : public Node
  {
  public:
    explicit ScanMaterialised(const std::string& expression = "")
      : Node(NodeType::SCAN_MATERIALISED)
      , expression(expression)
    {
    }

    std::string compactSymbolic() const override;;

    std::string renderMuggle(size_t max_width) const override;

    const std::string expression;
  };
} // namespace sql::explain
