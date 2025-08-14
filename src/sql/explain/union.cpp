#include "union.h"
static constexpr const char* symbol_ = "∪";

namespace sql::explain {
std::string Union::compactSymbolic() const {
  auto result = std::string(symbol_) + "{";
  if (type == Type::DISTINCT) {
    result += "distinct";
  } else {
    result += "all";
  }

  result += "}";
  return result;
}

std::string Union::renderMuggle() const {
  std::string result = "UNION ";
  if (type == Type::DISTINCT) {
    result += "DISTINCT";
  } else {
    result += "ALL";
  }
  return result;
}
}