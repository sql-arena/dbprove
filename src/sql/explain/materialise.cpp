#include "materialise.h"

namespace sql::explain {
std::string Materialise::compactSymbolic() const {
  std::string result = "MATERIALISE";
  result += " AS " + std::to_string(node_id);
  return result;
}

std::string Materialise::renderMuggle(size_t max_width) const {
  std::string result = "MATERIALISE";
  result += " AS " + std::to_string(node_id);
  return result;
}

std::string Materialise::treeSQLImpl(size_t indent) const {
  return firstChild()->treeSQL(indent);
}
}
