#include "connection.h"
#include "result.h"
#include <dbprove/sql/sql.h>
#include <nlohmann/json.hpp>
#include <duckdb.hpp>
#include <memory>
#include <regex>
#include <scan_materialised.h>
#include <stdexcept>

#include "sql_exceptions.h"


namespace sql::duckdb {
void handleDuckError(::duckdb::QueryResult* result) {
  if (!result->HasError()) {
    return;
  }
  const auto error_type = result->GetErrorType();
  switch (error_type) {
    case ::duckdb::ExceptionType::INVALID:
      break;
    case ::duckdb::ExceptionType::OUT_OF_RANGE:
      break;
    case ::duckdb::ExceptionType::CONVERSION:
      break;
    case ::duckdb::ExceptionType::DECIMAL:
      break;
    case ::duckdb::ExceptionType::MISMATCH_TYPE:
      break;
    case ::duckdb::ExceptionType::DIVIDE_BY_ZERO:
      break;
    case ::duckdb::ExceptionType::OBJECT_SIZE:
      break;
    case ::duckdb::ExceptionType::UNKNOWN_TYPE:
    case ::duckdb::ExceptionType::INVALID_TYPE:
      throw InvalidObjectException(result->GetError());
    case ::duckdb::ExceptionType::SERIALIZATION:
      break;
    case ::duckdb::ExceptionType::TRANSACTION:
      break;
    case ::duckdb::ExceptionType::NOT_IMPLEMENTED:
      throw NotImplementedException(result->GetError());
    case ::duckdb::ExceptionType::EXPRESSION:
      break;
    case ::duckdb::ExceptionType::CATALOG: {
      throw InvalidObjectException(result->GetError());
    }
    case ::duckdb::ExceptionType::PARSER:
      break;
    case ::duckdb::ExceptionType::PLANNER:
      break;
    case ::duckdb::ExceptionType::SCHEDULER:
      break;
    case ::duckdb::ExceptionType::EXECUTOR:
      break;
    case ::duckdb::ExceptionType::CONSTRAINT:
      break;
    case ::duckdb::ExceptionType::INDEX:
      break;
    case ::duckdb::ExceptionType::STAT:
      break;
    case ::duckdb::ExceptionType::CONNECTION:
      break;
    case ::duckdb::ExceptionType::SYNTAX:
      throw SyntaxException(result->GetError());
    case ::duckdb::ExceptionType::SETTINGS:
      break;
    case ::duckdb::ExceptionType::BINDER:
      break;
    case ::duckdb::ExceptionType::NETWORK:
      break;
    case ::duckdb::ExceptionType::OPTIMIZER:
      break;
    case ::duckdb::ExceptionType::NULL_POINTER:
      break;
    case ::duckdb::ExceptionType::IO:
      break;
    case ::duckdb::ExceptionType::INTERRUPT:
      break;
    case ::duckdb::ExceptionType::FATAL:
      break;
    case ::duckdb::ExceptionType::INTERNAL:
      break;
    case ::duckdb::ExceptionType::INVALID_INPUT:
      break;
    case ::duckdb::ExceptionType::OUT_OF_MEMORY:
      break;
    case ::duckdb::ExceptionType::PERMISSION:
      break;
    case ::duckdb::ExceptionType::PARAMETER_NOT_RESOLVED:
      break;
    case ::duckdb::ExceptionType::PARAMETER_NOT_ALLOWED:
      break;
    case ::duckdb::ExceptionType::DEPENDENCY:
      break;
    case ::duckdb::ExceptionType::HTTP:
      break;
    case ::duckdb::ExceptionType::MISSING_EXTENSION:
      break;
    case ::duckdb::ExceptionType::AUTOLOAD:
      break;
    case ::duckdb::ExceptionType::SEQUENCE:
      break;
    case ::duckdb::ExceptionType::INVALID_CONFIGURATION:
      break;
  }

  throw std::runtime_error("DuckDB query execution failed: " + result->GetError());
}

class Connection::Pimpl {
public:
  Connection& connection;
  const CredentialFile credential;
  std::unique_ptr<::duckdb::DuckDB> db;
  std::unique_ptr<::duckdb::Connection> db_connection;

  explicit Pimpl(Connection& connection, const CredentialFile& credential)
    : connection(connection)
    , credential(credential) {
    try {
      // Open a database connection using the file path from credential
      db = std::make_unique<::duckdb::DuckDB>(credential.path);
      db_connection = std::make_unique<::duckdb::Connection>(*db);
    } catch (const ::duckdb::IOException& e) {
      throw ConnectionException(credential, std::string(e.what()));
    } catch (const ::duckdb::Exception& e) {
      throw std::runtime_error("Failed to connect to DuckDB: " + std::string(e.what()));
    }
  }

  void close() {
    db_connection = nullptr;
  }

  void check_connection() const {
    if (!db_connection) {
      throw ConnectionClosedException(credential);
    }
  }

  [[nodiscard]] std::unique_ptr<::duckdb::QueryResult> execute(const std::string_view statement) const {
    check_connection();
    try {
      const auto mapped_statement = connection.mapTypes(statement);
      auto result = db_connection->Query(std::string(mapped_statement));

      handleDuckError(result.get());
      return result;
    } catch (const ::duckdb::ConnectionException& e) {
      throw ConnectionException(credential, std::string(e.what()));
    } catch (const ::duckdb::CatalogException& e) {
      throw std::runtime_error("DuckDB catalog error (table might not exist): " + std::string(e.what()));
    } catch (const ::duckdb::ParserException& e) {
      throw SyntaxException("DuckDB SQL parsing error: " + std::string(e.what()));
    } catch (const ::duckdb::BinderException& e) {
      throw std::runtime_error("DuckDB binding error (column mismatch): " + std::string(e.what()));
    } catch (const ::duckdb::IOException& e) {
      throw std::runtime_error("DuckDB I/O error accessing file: " + std::string(e.what()));
    } catch (const ::duckdb::TransactionException& e) {
      throw TransactionException("DuckDB transaction error: " + std::string(e.what()));
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

void Connection::execute(const std::string_view statement) {
  auto result = impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  auto result = impl_->execute(statement);
  return std::make_unique<Result>(std::move(result));
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
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

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);

  for (const auto& path : source_paths) {
    std::string copy_statement = "COPY " + std::string(table) + " FROM '" + path.string() + "'" "\nWITH (FORMAT 'csv', "
                                 "DELIM '|', " "AUTO_DETECT true, " "HEADER true, " "STRICT_MODE false " ")";
    auto result = impl_->execute(copy_statement);
  }
}


std::string Connection::version() {
  const auto version_string = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(v(\d+\.\d+.\d+))");
  std::smatch match;
  if (std::regex_search(version_string, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

void Connection::close() {
  impl_->close();
}
}