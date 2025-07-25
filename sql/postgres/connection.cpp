#include "connection.h"
#include "libpq-fe.h"
#include "../credential_password.h"
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
};

sql::postgres::Connection::Connection(const CredentialPassword& credential):
ConnectionBase(credential)
, impl_(std::make_unique<Pimpl>(*this, credential))
{
}

sql::postgres::Connection::~Connection() = default;

void sql::postgres::Connection::execute(std::string_view statement) {
}

std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchAll(std::string_view statement) {
  return nullptr;
}

std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchMany(std::string_view statement) {
  return nullptr;
}

std::unique_ptr<sql::RowBase> sql::postgres::Connection::fetchRow(std::string_view statement) {
  return nullptr;
}

sql::SqlVariant sql::postgres::Connection::fetchValue(std::string_view statement) {
  return SqlVariant(42);
}

void sql::postgres::Connection::
bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
}