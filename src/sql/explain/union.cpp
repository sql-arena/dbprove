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

std::string Union::renderMuggle(size_t max_width) const {
  std::string result = "UNION ";
  result += to_string(type);
  return result;
}

std::string Union::treeSQLImpl(size_t indent) const {
  std::string result = "SELECT * ";
  result += newline(indent);
  result += "FROM (";
  for (auto& child : children()) {
    result += child->treeSQL(indent + 1);
    if (child != children().back()) {
      result += newline(indent);
      result += "UNION";
      result += newline(indent);
      result += to_string(type);
    }
  }
  result += ") AS " + nodeName();
  return result;
}

std::string_view to_string(const Union::Type type) {
  return magic_enum::enum_name(type);
}
}