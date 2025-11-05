#include "projection.h"
#include "glyphs.h"
#include "group_by.h"

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

GroupBy* findDescendantAgg(const Node* n) {
  auto descendant = n->firstChild();
  while (true) {
    if (descendant->type == NodeType::GROUP_BY) {
      return dynamic_cast<GroupBy*>(descendant);
    }
    if (descendant->childCount() == 1) {
      descendant = descendant->firstChild();
      continue;
    }
    return nullptr;
  }
}

std::string replace(const std::string& input, const std::string& search, const std::string& replacement) {
  std::string result = input;
  size_t pos = 0;
  while ((pos = result.find(search, pos)) != std::string::npos) {
    result.replace(pos, search.length(), replacement);
    pos += replacement.length();
  }
  return result;
}


std::string Projection::treeSQLImpl(size_t indent) const {
  std::string result = newline(indent);
  result += "(SELECT * ";
  for (const auto c : columns_projected) {
    result += ", ";
    const auto descendant_agg = findDescendantAgg(this);
    if (descendant_agg) {
      std::string resolved = c.name;
      for (const auto& [a, v] : descendant_agg->aggregateAliases) {
        if (resolved.contains(a.name)) {
          resolved = replace(resolved, a.name, v);
        }
      }
      if (resolved != c.name) {
        if (c.hasAlias()) {
          result += resolved + " AS " + c.alias;
        } else {
          result += resolved;
        }
        continue;
      }
    }

    if (!c.hasAlias()) {
      result += c.name;
      continue;
    }
    result += c.name + " AS " + c.alias;
  }
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  result += ") AS " + subquerySQLAlias();
  return result;
}
}