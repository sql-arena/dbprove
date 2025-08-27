#include "connection.h"
#include <dbprove/sql/sql.h>
#include <memory>
#include <cassert>
#include <regex>

namespace sql::yellowbrick {
Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : postgres::Connection(credential, engine) {
}

std::string Connection::version() {
  const auto versionString = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(\d+\.\d+\.\d+)");
  std::smatch match;
  if (std::regex_search(versionString, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

std::string Connection::translateDialectDdl(const std::string_view ddl) const {
  std::string pg = postgres::Connection::translateDialectDdl(ddl);
  const std::regex re(";");
  auto r = std::regex_replace(pg, re, " WITH REPLICATE;");
  return r;
}

std::unique_ptr<explain::Plan> buildExplainPlan() {
  return nullptr;
}

std::unique_ptr<explain::Plan> Connection::explain(std::string_view statement) {
  const std::string explain_modded = "EXPLAIN (ANALYSE, VERBOSE, FORMAT YBXML)\n" + std::string(statement);
  const auto result = fetchScalar(explain_modded);
  assert(result.is<SqlString>());

  auto explain_string = result.get<SqlString>().get();

  return buildExplainPlan();
}
}