#include "projection.h"

namespace sql::explain {
std::string Projection::compactSymbolic() const {
  std::string result;
  result += symbol_;
  result += "{";
  result += Column::join(columns_projected, ", ");
  result += "}";
  return result;
}

std::string Projection::renderMuggle() const {
  std::string result = "PROJECT ";
  result += "(";
  result += Column::join(columns_projected, ", ");
  result += ")";
  return result;
}
}