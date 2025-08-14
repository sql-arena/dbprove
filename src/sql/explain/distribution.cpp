#include "distribution.h"
#include "explain/column.h"

constexpr auto symbol_broadcast_ = "⟨B⟩";
constexpr auto symbol_distribute_ = "⟨R⟩";
constexpr auto symbol_gather_ = "⟨G⟩";
constexpr auto symbol_round_robin_ = "⟨D⟩";

std::string sql::explain::Distribute::compactSymbolic() const {
  std::string result;
  switch (strategy) {
    case Strategy::HASH:
      result += symbol_distribute_;
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
}