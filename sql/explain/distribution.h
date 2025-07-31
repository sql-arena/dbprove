#pragma once
#include "node.h"
#include "node_type.h"

namespace sql::explain {
class Column;

class Distribute : public Node {
  constexpr const char* symbol_broadcast_ = "⟨B⟩";
  constexpr const char* symbol_distribute_ = "⟨R⟩";
  constexpr const char* symbol_gather_ = "⟨G⟩";
  constexpr const char* symbol_round_robin_ = "⟨D⟩";

public:
  enum class Strategy {
    HASH, BROADCAST, ROUND_ROBIN, GATHER
  };

  explicit Distribute(const Strategy strategy, const std::vector<Column>& columns_distribute = {})
    : Node(NodeType::DISTRIBUTE)
    , columns_distribute(columns_distribute)
    , strategy(strategy) {
  }

  std::string compactSymbolic() const override {
    std::string result;
    switch (strategy) {
      case Strategy::HASH:
        result += symobl_distribute_;
        result += "{";
        for (const auto& column : columns_distribute) {
          result += column.name;
        }
        result += "}";
        break;
      case Strategy::BROADCAST:
        result+=symbol_broadcast_ ;
        break;
      case Strategy::ROUND_ROBIN:
        result += symbol_round_robin_;
        break;
      case Strategy::GATHER:
        result += symbol_gather_;
    }
    return result;
  };

  std::vector<Column> columns_distribute;
  Strategy strategy;
};
} // namespace sql::explain