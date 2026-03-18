#pragma once
#include "explain/node.h"
#include <string>

namespace sql::explain {
class Materialise;

/**
 * A scan of a materialised result set from another part of the query.
 */
class ScanMaterialised final : public Node {
public:
  explicit ScanMaterialised(int primary_node_id = -1,
                            const std::string& expression = "",
                            const std::string& primary_node_name = "")
    : Node(NodeType::SCAN_MATERIALISED)
    , primary_node_id(primary_node_id)
    , expression(expression)
    , primary_node_name(primary_node_name) {
  }

  std::string compactSymbolic() const override;;

  std::string renderMuggle(size_t max_width) const override;
  std::string materialisedAlias() const;
  void setSourceMaterialise(const Materialise* source);
  const Materialise* sourceMaterialise() const;
  std::string actualsSql() override;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  const int primary_node_id;
  const std::string expression;
  const std::string primary_node_name;
  const Materialise* source_materialise = nullptr;
};
} // namespace sql::explain
