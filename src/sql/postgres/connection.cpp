#include "connection.h"
#include "row.h"
#include <libpq-fe.h>
#include "result.h"
#include <dbprove/sql/sql.h>
#include <explain_nodes.h>
#include <nlohmann/json.hpp>

#include <cassert>
#include <fstream>
#include <regex>


class sql::postgres::Connection::Pimpl {
public:
  Connection& connection;
  const CredentialPassword credential;
  PGconn* conn;

  explicit Pimpl(Connection& connection, const CredentialPassword& credential)
    : connection(connection)
    , credential(credential) {
    std::string connection_string = "host=" + credential.host + " dbname=" + credential.database + " user=" + credential
                                    .username + " port=" + std::to_string(credential.port);
    if (credential.password.has_value()) {
      connection_string += " password=" + credential.password.value();
    }
    conn = PQconnectdb(connection_string.c_str());

    check_connection();
  }

  void safeClose() {
    if (conn) {
      PQfinish(conn);
      conn = nullptr;
    }
  }

  void check_connection() {
    if (conn == nullptr) {
      throw ConnectionClosedException(credential);
    }

    if (PQstatus(conn) != CONNECTION_OK) {
      const std::string error = PQerrorMessage(conn);
      safeClose();
      throw ConnectionException(credential, error);
    }
  }

  /// @brief Handles return values from calls into postgres
  /// @note: If we throw here, we will call `PQClear`. But it is the responsibility of the caller to clear on success
  void check_return(PGresult* result, const std::string_view statement) const {
    // ReSharper disable once CppTooWideScope
    const auto status = PQresultStatus(result);
    switch (status) {
      case PGRES_TUPLES_OK:
      case PGRES_COMMAND_OK:
      case PGRES_EMPTY_QUERY:
      case PGRES_SINGLE_TUPLE:
      case PGRES_COPY_IN: // Ready to receive data on COPY
        return;
      /* Legit and harmless status code */

      default:
        const std::string error_msg = PQerrorMessage(this->conn);
        assert(!error_msg.empty());
        const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        std::string state;
        if (sqlstate == nullptr) {
          state = "UNKNOWN";
        } else {
          state = sqlstate;
        }
        PQclear(result);

        if (state.starts_with("42P01") // The Table  already exists
            || state.starts_with("42710") // The Foreign Key already exists
        ) {
          throw InvalidObjectException(error_msg);
        }
        if (state.starts_with("42")) {
          throw SyntaxException(error_msg, statement);
        }
        throw ConnectionException(credential, error_msg);
    }
  }

  PGresult* execute(const std::string_view statement) {
    check_connection();
    const auto mapped_statement = connection.mapTypes(statement);
    PGresult* result = PQexecParams(conn, mapped_statement.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 1);
    check_return(result, statement);
    return result;
  }

  [[maybe_unused]] auto executeRaw(const std::string_view statement) {
    check_connection();
    const auto mapped_statement = connection.mapTypes(statement);
    PGresult* result = PQexec(conn, mapped_statement.data());
    const auto status = PQresultStatus(result);
    check_return(result, statement);
    PQclear(result);
    return status;
  }
};

sql::postgres::Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(*this, credential)) {
}

const sql::ConnectionBase::TypeMap& sql::postgres::Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT8"}, {"STRING", "VARCHAR"}};
  return map;
}

sql::postgres::Connection::~Connection() {
  PQfinish(impl_->conn);
}

void sql::postgres::Connection::execute(const std::string_view statement) {
  [[maybe_unused]] auto s = impl_->executeRaw(statement);
}

std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchAll(const std::string_view statement) {
  PGresult* result = impl_->execute(statement);
  return std::make_unique<Result>(result);
}

void check_bulk_return(const int status, const PGconn* conn) {
  if (status != 1) {
    throw std::runtime_error("Failed to send data to the database " + std::string(PQerrorMessage(conn)));
  }
}

void sql::postgres::Connection::bulkLoad(const std::string_view table,
                                         const std::vector<std::filesystem::path> source_paths) {
  /*
   * Copying data into Postgres:
   *
   * This interfaces is so obscenely braindead that you simply have to read the code.
   * Basically, stuff can fail at any time during copy and how you exactly get the error message
   * and handle it depends on what part of the flow you are in.
   *
   * The aim here is to turn the PG errors into subclasses of sql::Exception and give them some
   * decent error codes and error messages
   */

  validateSourcePaths(source_paths);

  const auto cn = impl_->conn;
  for (const auto& path : source_paths) {
    std::ifstream file(path.string(), std::ios::binary);
    if (!file.is_open()) {
      throw std::ios_base::failure("Failed to open source file: " + path.string());
    }

    // First, we need to tell PG that a copy stream is coming. This puts the server into a special mode
    std::string copy_query = "COPY " + std::string(table) + " FROM STDIN" +
                             " WITH (FORMAT csv, DELIMITER '|', NULL '', HEADER)";
    const auto ready_status = impl_->executeRaw(copy_query);
    assert(ready_status == PGRES_COPY_IN); // We better have handled this already

    // Chunk file content to the database (synchronously) with 1MB chunks so we can hide latency
    // There is an "async" interface too - but it requires polling sockets and manually backing off
    // based on return codes. One day, I will make this work - after I lose my will to live!
    constexpr size_t buffer_size = 1024 * 1024;
    auto buf = std::make_unique<char[]>(buffer_size);
    while (file) {
      file.read(buf.get(), buffer_size);
      const std::streamsize n = file.gcount();
      if (n > 0) {
        const auto progress_status = PQputCopyData(cn, buf.get(), static_cast<int>(n));
        check_bulk_return(progress_status, cn);
      }
    }
    const auto end_status = PQputCopyEnd(cn, nullptr);
    check_bulk_return(end_status, cn);

    // After our final row (which is marked by PQOutCopyEnd) we now get a result back telling us if it worked
    const auto final_result = PQgetResult(cn);
    impl_->check_return(final_result, copy_query);
    PQclear(PQgetResult(cn));
    // Drain the connection - the usual libpq pointless logic
    while (PGresult* leftover = PQgetResult(cn)) {
      PQclear(leftover);
    }
  }
}


std::string sql::postgres::Connection::version() {
  const auto versionString = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(PostgreSQL (\d+\.\d+))");
  std::smatch match;
  if (std::regex_search(versionString, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

void sql::postgres::Connection::close() {
  impl_->safeClose();
}