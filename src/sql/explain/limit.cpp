#include "limit.h"

std::string sql::explain::Limit::treeSQLImpl(const size_t indent) const {
  std::string result = "(SELECT *";
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  result += "LIMIT " + std::to_string(limit_count) + ") AS " + subquerySQLAlias();
  return result;
}