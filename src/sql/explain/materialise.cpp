#include "materialise.h"

namespace sql::explain {
std::string Materialise::materialisedAlias() const {
  if (!materialised_node_name.empty()) {
    return "m_" + materialised_node_name;
  }
  if (node_id >= 0) {
    return "m" + std::to_string(node_id);
  }
  return "m" + std::to_string(id());
}

std::string Materialise::compactSymbolic() const {
  std::string result = "MATERIALISE";
  result += " AS " + materialisedAlias();
  return result;
}

std::string Materialise::renderMuggle(size_t max_width) const {
  std::string result = "MATERIALISE";
  result += " AS " + materialisedAlias();
  return result;
}

std::string Materialise::treeSQLImpl(size_t indent) const {
  std::string result = newline(indent);
  result += "(SELECT * ";
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  result += ") AS " + materialisedAlias();
  return result;
}
}
