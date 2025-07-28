#include "connection.h"

#include <cassert>
#include "row.h"
#include "libpq-fe.h"
#include "../credential.h"
#include "../sql_exceptions.h"

class sql::postgres::Connection::Pimpl {
public:
  Connection& connection;
  const CredentialPassword& credential;
  PGconn* conn;
  explicit Pimpl(Connection& connection, const CredentialPassword& credential): connection(connection), credential(credential) {

    std::string conninfo =
      "dbname=" + credential.database +
      " user=" + credential.username +
      " password=" + credential.password +
      " port=" + std::to_string(credential.port);
    conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
      std::string error = PQerrorMessage(conn);
      PQfinish(conn);
      throw ConnectionException(credential, error);
    }
  }

  /// @brief Handles return values from calls into postgres
  /// @note: If we throw here, we will call `PQClear`. But it is the responsibility of the caller to clear on success
  void check_return(PGresult* result) const {
    // ReSharper disable once CppTooWideScope
    const auto status = PQresultStatus(result);
    switch (status) {
      case PGRES_TUPLES_OK:
      case PGRES_COMMAND_OK:
      case PGRES_EMPTY_QUERY:
      case PGRES_SINGLE_TUPLE:
        /* Legit and harmless status code */
        return;
      default:
        std::string error_msg = PQerrorMessage(this->conn);
        assert( error_msg.size() > 0 );
        PQclear(result);
        throw sql::ConnectionException(this->credential, error_msg);
    }
  }
};


sql::postgres::Connection::Connection(const CredentialPassword& credential):
ConnectionBase(credential)
, impl_(std::make_unique<Pimpl>(*this, credential))
{
}

const sql::ConnectionBase::TypeMap& sql::postgres::Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT8"}, {"STRING", "TEXT"} };
  return map;
}

sql::postgres::Connection::~Connection() = default;

void sql::postgres::Connection::execute(std::string_view statement) {\
  PGresult* result = PQexec(impl_->conn, std::string(statement).c_str());
  impl_->check_return(result);
  PQclear(result);
}

std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchAll(std::string_view statement) {
  return nullptr;
}


std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchMany(std::string_view statement) {
  return nullptr;
}

std::unique_ptr<sql::RowBase> sql::postgres::Connection::fetchRow(std::string_view statement) {
  const auto mapped_statement = mapTypes(statement);
  PGresult* result = PQexecParams(impl_->conn,
    mapped_statement.c_str(),
    0,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    1);
  impl_->check_return(result);
  int rows = PQntuples(result);
  if (rows == 0) {
    PQclear(result);
    throw EmptyResultException(statement);
  }
  return std::make_unique<Row>(result);
}

sql::SqlVariant sql::postgres::Connection::fetchValue(std::string_view statement) {
  return SqlVariant(42);
}

void sql::postgres::Connection::bulkLoad(
  std::string_view table,
  const std::vector<std::filesystem::path>& source_paths) {
}