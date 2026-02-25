#include "connection.h"
#include "result.h"
#include "sql_exceptions.h"
#include <sqlite3.h>

namespace sql::sqlite {
class Connection::Pimpl {
public:
  sqlite3* db = nullptr;
  std::string db_path;

  static CredentialNone& no_creds() {
    static CredentialNone none;
    return none;
  }

  explicit Pimpl(std::string db_path)
    : db_path(std::move(db_path)) {
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
      throw ConnectionException(no_creds(), "Failed to open SQLite database: " + std::string(sqlite3_errmsg(db)));
    }
  }

  static void check_return(const int ret, char* error_message) {
    if (ret != SQLITE_OK) {
      const auto error_string = std::string(error_message);
      sqlite3_free(error_message);
      throw Exception(SqlState::INVALID, error_string);
      // TODO: Break this into its component errors
    }
  }

  void check_connection_open() const {
    if (!db) {
      throw ConnectionClosedException(no_creds());
    }
  }

  void executeRaw(const std::string_view statement) const {
    check_connection_open();
    char* error_message = nullptr;
    const auto results = sqlite3_exec(db, statement.data(), nullptr, nullptr, &error_message);
    check_return(results, error_message);
  }

  std::unique_ptr<Result> execute(const std::string_view statement) const {
    sqlite3_stmt* stmt = nullptr;
    check_return(sqlite3_prepare_v2(db, statement.data(), -1, &stmt, nullptr), nullptr);
    return std::make_unique<Result>(stmt);
  }

  void close() {
    if (db) {
      sqlite3_close(db);
      db = nullptr;
    }
  }

  ~Pimpl() {
    close();
  }
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(std::get<CredentialFile>(credential).path)) {
}

Connection::~Connection() {
}

void Connection::execute(std::string_view statement) {
  impl_->executeRaw(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  return impl_->execute(statement);
}


void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
}

std::string Connection::version() {
  return sqlite3_libversion();
}

void Connection::close() {
  impl_->close();
  ConnectionBase::close();
}
}