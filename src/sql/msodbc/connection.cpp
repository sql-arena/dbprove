#include "connection.h"
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
    throw NotImplementedException("Currently, only password credentials for SQL Server drivers");  }
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


const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT(53)"}, {"STRING", "VARCHAR"}};
  return map;
}

void Connection::analyse(const std::string_view table_name) {
  execute("UPDATE STATISTICS " + std::string(table_name));
}
}