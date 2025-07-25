#include "connection.h"
#include "../credential_base.h"
#include "../sql_exceptions.h"
#include "../connection_base.h"

class sql::msodbc::Connection::Pimpl {
public:
  Connection& connection;
  const CredentialBase& credential;

  explicit Pimpl(Connection& connection)
    : connection(connection)
    , credential(connection.credential) {
  }
};

sql::msodbc::Connection::Connection(const CredentialBase& credential)
  : ConnectionBase(credential)
  , impl_(std::make_unique<Pimpl>(*this)) {
}

sql::msodbc::Connection::~Connection() = default;

void sql::msodbc::Connection::execute(std::string_view statement) {
}

std::unique_ptr<sql::ResultBase> sql::msodbc::Connection::fetchAll(std::string_view statement) {
  return nullptr;
}

std::unique_ptr<sql::ResultBase> sql::msodbc::Connection::fetchMany(std::string_view statement) {
  return nullptr;
}

std::unique_ptr<sql::RowBase> sql::msodbc::Connection::fetchRow(std::string_view statement) {
  return nullptr;
}

sql::SqlVariant sql::msodbc::Connection::fetchValue(std::string_view statement) {
  return SqlVariant(42);
}

void sql::msodbc::Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
}