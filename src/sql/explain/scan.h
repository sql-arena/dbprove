#pragma once
#include "explain/node.h"

namespace sql::explain {
class Scan : public Node {
public:
  explicit Scan(const std::string& table_name);

  std::string compactSymbolic() const override;;

  std::string renderMuggle(size_t max_width) const override;

  const std::string table_name;
};
} // namespace sql::explain
