#include "connection.h"

#include <regex>

#include "sql_exceptions.h"

#include <vincentlaucsb-csv-parser/internal/csv_reader.hpp>
// The SQL odbc library needs some strange INT definitions
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint.h>
#endif

#include <mutex>
#include <string>
#include <stdexcept>


namespace sql::msodbc {
std::string makeConnectionString(const Credential& credential) {
  if (!std::holds_alternative<CredentialPassword>(credential)) {
    throw NotImplementedException("Currently, only password credentials for SQL Server drivers");
  }
  const auto pwd = std::get<CredentialPassword>(credential);

  std::string r = "DRIVER={ODBC Driver 18 for SQL Server};";
  r += "pooling=No;Encrypt=No;";
  r += "SERVER=" + pwd.host + ";";
  r += "DATABASE=" + pwd.database + ";";
  r += "UID=" + pwd.username + ";";
  r += "PWD=" + pwd.password.value_or("") + ";";
  return r;
}

Connection::Connection(const Credential& credential, const Engine& engine)
  : odbc::Connection(credential, engine, makeConnectionString(credential)) {
}


std::string Connection::version() {
  const auto version = fetchScalar("SELECT @@VERSION AS v");
  return version.asString();
}

/**
 * Translate from the generic ANSI syntax to SQL
 * @param sql
 * @return
 */
std::string translateSQL(std::string_view sql) {
  /* Limit is TOP
   * The translation here is (for now) simple. We can look for queries ending with LIMIT and put top next to SELECT
   * This is obviously error prone, but good enough for TPC-H
   * Ideally, we will want to parse the query
   */
  std::string query(sql);
  std::smatch match;
  std::regex limit_regex(R"(LIMIT\s+(\d+)\s*;?\s*$)", std::regex_constants::icase);

  if (std::regex_search(query, match, limit_regex)) {
    // Find and remove the LIMIT clause
    std::string limit_val = match[1];
    query = std::regex_replace(query, limit_regex, "");
    // Insert TOP x after SELECT
    std::regex select_regex(R"((SELECT\s+))", std::regex_constants::icase);
    query = std::regex_replace(query, select_regex, "$1TOP " + limit_val + " ",
                               std::regex_constants::format_first_only);
  }

  // Replace EXTRACT(YEAR FROM x) with YEAR(x)
  std::regex extract_year_regex(R"(EXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+)\))", std::regex_constants::icase);
  query = std::regex_replace(query, extract_year_regex, "YEAR($1)");

  return query;
}

void Connection::execute(const std::string_view statement) {
  odbc::Connection::execute(translateSQL(statement));
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  return odbc::Connection::fetchAll(translateSQL(statement));
}

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT(53)"}, {"STRING", "VARCHAR"}};
  return map;
}

void Connection::analyse(const std::string_view table_name) {
  execute("UPDATE STATISTICS " + std::string(table_name));
}
}