#include "connection.h"
#include <dbprove/sql/sql.h>
#include <regex>

namespace sql::cedardb {
std::string Connection::version() {
  const auto v = fetchScalar("SELECT version()").get<SqlString>().get();
  // "PostgreSQL 16.3 compatible CedarDB v2026-06-23, ..."
  const std::regex re(R"(CedarDB\s+(v[\d-]+))");
  std::smatch m;
  if (std::regex_search(v, m, re)) {
    return m[1];
  }
  return "Unknown";
}
// Connection::explain() is implemented in explain.cpp
} // namespace sql::cedardb
