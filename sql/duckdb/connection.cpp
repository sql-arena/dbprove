#include "connection.h"
#include "result.h"
#include <nlohmann/json.hpp>
#include "credential.h"
#include <duckdb.hpp>
#include <memory>
#include <stdexcept>



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

void Connection::execute(std::string_view statement) {
  auto result = impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(std::string_view statement) {
  auto result = impl_->execute(statement);
  return std::make_unique<Result>(std::move(result));
}
}