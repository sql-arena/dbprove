#include "selection.h"

namespace sql::explain {
Selection::Selection(const std::string& filter_expression, const EngineDialect* dialect)
  : Node(NodeType::FILTER) {
  setFilter(filter_expression, dialect);
}

std::string Selection::treeSQLImpl(const size_t indent) const {
  std::string select_list;
  if (explicit_columns.empty()) {
    select_list = "*";
  } else {
    for (size_t i = 0; i < explicit_columns.size(); ++i) {
      if (i > 0) select_list += ", ";
      select_list += explicit_columns[i];
    }
  }
  std::string result = newline(indent);
  result += "(SELECT " + select_list + " ";
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  const auto filter = syntheticFilterCondition().empty() ? filterCondition() : syntheticFilterCondition();
  if (!filter.empty()) {
    result += newline(indent);
    result += "WHERE " + filter;
  }
  result += ") AS " + subquerySQLAlias();
  return result;
}
}
