#include "select.h"

namespace sql::explain {
std::string Select::treeSQLImpl(const size_t indent) const {
  if (childCount() == 0) {
    return newline(indent) + "(SELECT 1) AS " + subquerySQLAlias();
  }
  return Node::treeSQLImpl(indent);
}

std::string Select::actualsSql() {
  if (childCount() == 0) {
    return "/* DBPROVE_ACTUALS */\nSELECT 1";
  }
  return Node::actualsSql();
}
}
