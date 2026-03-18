#pragma once
#include "explain/node.h"
#include <string>

namespace sql::explain {
/**
 * Represents the creation/writing of a materialized result set.
 */
class Materialise final : public Node {
public:
  explicit Materialise(const std::string& name = "",
                       int node_id = -1,
                       const std::string& materialised_node_name = "")
    : Node(NodeType::MATERIALISE)
    , name(name)
    , node_id(node_id)
    , materialised_node_name(materialised_node_name) {
  }

  std::string compactSymbolic() const override;
  std::string renderMuggle(size_t max_width) const override;
  std::string materialisedAlias() const;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  const std::string name;
  const int node_id;
  const std::string materialised_node_name;
};
} // namespace sql::explain
