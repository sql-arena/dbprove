#pragma once
#include "explain/node.h"
#include <string>

namespace sql::explain {
/**
 * Represents the creation/writing of a materialized result set.
 */
class Materialise final : public Node {
public:
  explicit Materialise(const std::string& name = "", int node_id = -1)
    : Node(NodeType::MATERIALISE)
    , name(name)
    , node_id(node_id) {
  }

  std::string compactSymbolic() const override;
  std::string renderMuggle(size_t max_width) const override;

protected:
  std::string treeSQLImpl(size_t indent) const override;

public:
  const std::string name;
  const int node_id;
};
} // namespace sql::explain
