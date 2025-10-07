#include "projection.h"
#include "glyphs.h"

namespace sql::explain {
std::string Projection::compactSymbolic() const {
  std::string result;
  result += symbol_;
  result += "{";
  result += join(columns_projected, ", ");
  result += "}";
  return result;
}

std::string Projection::renderMuggle(size_t max_width) const {
  // TODO: This needs fixing for max width
  std::string result = "PROJECT ";
  max_width -= result.size();
  result += join(columns_projected, ", ", max_width - 1);
  return result;
}
}