#include <vector>
// The SQL odbc library needs some strange INT definitions
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "connection.h"
#include "result.h"
#include "sql_exceptions.h"

using namespace sql;

namespace sql::msodbc {
class Connection::Pimpl {
  SQLHENV env = nullptr;
  SQLHDBC connection = nullptr;
  Engine engine;
  CredentialPassword credential;
  std::string connection_string;

public:
  void check_connection(const SQLRETURN ret, const SQLHANDLE handle, const SQLSMALLINT type) {
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
      return;

    SQLCHAR state[6], msg[256];
    SQLINTEGER native;
    SQLSMALLINT len;
    SQLGetDiagRec(type, handle, 1, state, &native, msg, sizeof(msg), &len);

    std::string error = "Failed to open connection with state: ";
    error += reinterpret_cast<char*>(state);
    error += " - message: " + std::string(reinterpret_cast<char*>(msg));
    throw ConnectionException(credential, error);
  }

  explicit Pimpl(const Engine engine, CredentialPassword credential)
    : engine(engine)
    , credential(credential) {
    check_connection(
        SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE, &env),
        env, SQL_HANDLE_ENV);

    check_connection(
        SQLSetEnvAttr(env,SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0)
        , env, SQL_HANDLE_ENV);

    check_connection(
        SQLAllocHandle(SQL_HANDLE_DBC, env, &connection)
        , connection, SQL_HANDLE_DBC);

    // Build connection string
    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + credential.host +
                        ";DATABASE=" + credential.database +
                        ";UID=" + credential.username +
                        ";PWD=" + credential.password.value_or("") + ";";

    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen;
    check_connection(
        SQLDriverConnect(
            connection, nullptr,
            reinterpret_cast<SQLCHAR*>(const_cast<char*>((connection_string.c_str()))),
            SQL_NTS,
            outConnStr, sizeof(outConnStr), &outConnStrLen,
            SQL_DRIVER_NOPROMPT
            )
        , connection, SQL_HANDLE_DBC
        );
  }
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(engine, std::get<CredentialPassword>(credential))) {
}

Connection::~Connection() {
  close();
}

void Connection::execute(std::string_view statement) {
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  // TODO: Talk to the engine and acquire the memory structure of the result, then pass it to result
  return std::make_unique<Result>(nullptr);
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  // TODO: Implement result set scrolling
  return fetchAll(statement);
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
  return nullptr;
}

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
}

std::string Connection::version() {
  const auto version = fetchScalar("SELECT @@VERSION");
  return version.asString();
}

void Connection::close() {
  ConnectionBase::close();
}
}