#include "projection.h"
#include "glyphs.h"
#include "group_by.h"

namespace sql::explain {
Projection::Projection(const std::vector<Column>& columns_projected)
  : Node(NodeType::PROJECTION)
  , columns_projected(columns_projected) {
}

std::string Projection::compactSymbolic() const {
  std::string result;
  result += symbol_;
  result += "{";
  result += join(columns_projected, ", ");
  result += "}";
  return result;
}

std::string Projection::renderMuggle(size_t max_width) const {
  std::string result = "PROJECT ";
  if (max_width > result.size()) {
    max_width -= result.size();
  }
  std::string rendered_columns;
  for (size_t i = 0; i < columns_projected.size(); ++i) {
    if (i > 0) {
      rendered_columns += ", ";
    }
    rendered_columns += columns_projected[i].name;
    if (columns_projected[i].hasAlias()) {
      rendered_columns += " AS ";
      rendered_columns += columns_projected[i].alias;
    }
  }
  result += ellipsify(rendered_columns, max_width > 0 ? max_width - 1 : max_width);
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
  const auto& projected = synthetic_columns_projected.empty() ? columns_projected : synthetic_columns_projected;
  std::string result = newline(indent);
  result += "(SELECT ";
  if (include_input_columns || projected.empty()) {
    result += "*";
  }
  for (const auto c : projected) {
    if (result.back() != ' ') {
      result += ", ";
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
