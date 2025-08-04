#include "connection.h"
#include "result.h"
#include <nlohmann/json.hpp>
#include "credential.h"
#include <duckdb.hpp>
#include <memory>
#include <stdexcept>

#include "sql_exceptions.h"


namespace sql::duckdb {
class Connection::Pimpl {
public:
  Connection& connection;
  const sql::CredentialFile credential;
  std::unique_ptr<::duckdb::DuckDB> db;
  std::unique_ptr<::duckdb::Connection> db_connection;


  explicit Pimpl(Connection& connection, const sql::CredentialFile& credential)
    : connection(connection)
    , credential(credential) {
    try {
      // Open database connection using the file path from credential
      db = std::make_unique<::duckdb::DuckDB>(credential.path);
      db_connection = std::make_unique<::duckdb::Connection>(*db);
    } catch (const ::duckdb::Exception& e) {
      throw std::runtime_error("Failed to connect to DuckDB: " + std::string(e.what()));
    }
  }

  std::unique_ptr<::duckdb::QueryResult> execute(const std::string_view statement) const {
    try {
      const auto mapped_statement = connection.mapTypes(statement);
      auto result = db_connection->Query(std::string(mapped_statement));

      // Check for errors using the public API
      if (result->HasError()) {
        throw std::runtime_error("DuckDB query execution failed: " + result->GetError());
      }
      return result;
    } catch (const ::duckdb::Exception& e) {
      throw std::runtime_error("Error executing DuckDB query: " + std::string(e.what()));
    }
  }
};

Connection::Connection(const CredentialFile& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(*this, credential)) {
}

const ConnectionBase::TypeMap& Connection::typeMap() const {
  // Duck has great type aliasing, so no need to map
  static const TypeMap map = {};
  return map;
}

Connection::~Connection() {
}

void Connection::execute(std::string_view statement) {
  auto result = impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(std::string_view statement) {
  auto result = impl_->execute(statement);
  return std::make_unique<Result>(std::move(result));
}

std::unique_ptr<ResultBase> Connection::fetchMany(std::string_view statement) {
  return fetchAll(statement);
}

std::unique_ptr<RowBase> Connection::fetchRow(std::string_view statement) {
  auto result = new Result(impl_->execute(statement));
  const auto row_count = result->rowCount();
  if (row_count == 0) {
    throw EmptyResultException(statement);
  }
  if (row_count > 1) {
    throw InvalidRowsException("Expected to find a single row in the data, but found: " + std::to_string(row_count),
                               statement);
  }
  auto& firstRow = result->nextRow();
  auto row = std::make_unique<Row>(static_cast<const Row&>(firstRow), result);
  return row;
}

SqlVariant Connection::fetchScalar(std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(std::string_view table, const std::vector<std::filesystem::path>& source_paths) {
}

std::unique_ptr<explain::Plan> Connection::explain(std::string_view statement) {
  return ConnectionBase::explain(statement);
}
}