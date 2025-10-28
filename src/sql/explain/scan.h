#pragma once
#include "explain/node.h"

namespace sql::explain {
class Scan final : public Node {
public:
  enum class Strategy { SCAN, SEEK };

  explicit Scan(const std::string& table_name, Strategy strategy = Strategy::SCAN);

  std::string compactSymbolic() const override;;

  std::string renderMuggle(size_t max_width) const override;

  const Strategy strategy;
  const std::string table_name;
};
} // namespace sql::explain
