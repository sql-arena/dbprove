#include <cassert>
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

  void check_return(const SQLRETURN return_value, SQLHANDLE handle) {
    if (return_value == SQL_SUCCESS
        || return_value == SQL_SUCCESS_WITH_INFO
        || return_value == SQL_NO_DATA
    ) {
      return;
    }
    assert(return_value != SQL_INVALID_HANDLE);
    assert(return_value != SQL_STILL_EXECUTING);

    SQLCHAR state_buf[6] = {};
    SQLCHAR message_buf[256] = {};
    SQLINTEGER nativeError = 0;
    SQLSMALLINT textLength = 0;

    const auto ret = SQLGetDiagRec(SQL_HANDLE_STMT,
                                   handle, 1,
                                   state_buf,
                                   &nativeError,
                                   message_buf,
                                   sizeof(message_buf),
                                   &textLength);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      throw std::runtime_error("Failed to retrieve error information");
    }
    const std::string state = reinterpret_cast<const char*>(state_buf);
    const std::string message = reinterpret_cast<const char*>(message_buf);
    if (state.rfind("08", 0)) {
      throw ConnectionException(credential, message);
    }
  }

  void check_connection_not_closed() {
    if (!connection) {
      throw ConnectionClosedException(credential);
    }
  }

  void executeRaw(const std::string_view statement) {
    check_connection_not_closed();
    SQLHSTMT statement_handle = nullptr;
    check_connection(
        SQLAllocHandle(SQL_HANDLE_STMT, connection, &statement_handle),
        connection, SQL_HANDLE_DBC
        );

    const auto ret = SQLExecDirect(statement_handle,
                                   reinterpret_cast<SQLCHAR*>(const_cast<char*>(statement.data())),
                                   static_cast<SQLINTEGER>(statement.size()));
    check_return(ret, statement_handle);
    SQLFreeHandle(SQL_HANDLE_STMT, statement_handle);
  }

  std::unique_ptr<Result> execute(const std::string_view statement) {
    check_connection_not_closed();
    SQLHSTMT statement_handle = nullptr;
    check_connection(
        SQLAllocHandle(SQL_HANDLE_STMT, connection, &statement_handle),
        connection, SQL_HANDLE_DBC
        );
    const auto ret = SQLExecDirect(
        statement_handle,
        reinterpret_cast<SQLCHAR*>(const_cast<char*>(statement.data())),
        static_cast<SQLINTEGER>(statement.size())
        );
    check_return(ret, statement_handle);
    return std::make_unique<Result>(statement_handle);
  }

  void close() {
    if (!connection) {
      return;
    }
    SQLDisconnect(connection);
    SQLFreeHandle(SQL_HANDLE_DBC, connection);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    connection = nullptr;
  }
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(engine, std::get<CredentialPassword>(credential))) {
}

Connection::~Connection() {
  close();
}

void Connection::execute(const std::string_view statement) {
  impl_->executeRaw(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  return impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  return fetchAll(statement);
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
  /* Because the TDS protocol is streaming, we don't know the number of rows until we have scrolled though the cursor */
  const auto result = fetchAll(statement);
  std::unique_ptr<MaterialisedRow> first_row = nullptr;
  for (auto& row : result->rows()) {
    if (first_row) {
      throw InvalidRowsException("Expected to find a single row in the data, but found more than one", statement);
    }
    first_row = row.materialise();
  }
  if (!first_row) {
    throw EmptyResultException(statement);
  }
  return first_row;
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
}

std::string Connection::version() {
  const auto version = fetchScalar("SELECT @@VERSION");
  return version.asString();
}

void Connection::close() {
  impl_->close();
  ConnectionBase::close();
}
}