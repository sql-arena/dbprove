#include "connection.h"
#include "result.h"
#include "sql_exceptions.h"
#include <mysql/mysql.h>


namespace sql::mariadb {
class Connection::Pimpl {
public:
  CredentialPassword credential;
  Engine engine;
  MYSQL* conn;

  explicit Pimpl(CredentialPassword credential, Engine engine)
    : credential(credential)
    , engine(engine) {
    conn = mysql_init(nullptr);
    if (!conn) {
      throw std::runtime_error("Failed to initialize construct MySQL connection");
    }
    if (!mysql_real_connect(conn,
                            credential.host.c_str(),
                            credential.username.c_str(),
                            credential.password.value_or("").c_str(),
                            credential.database.c_str(),
                            credential.port,
                            nullptr,
                            0)) {
      std::string error_msg = mysql_error(conn);
      throw ConnectionException(credential, error_msg);
    }
  }

  void check_connection_not_closed() {
    if (!conn) {
      throw ConnectionClosedException(credential);
    }
  }

  void check_error(int error) {
    if (error == 0) {
      return;
    }
    const std::string error_msg = mysql_error(conn);
    const auto error_code = mysql_errno(conn);
    switch (error_code) {
      // TODO: Fill this in
      default:
        throw Exception(SqlState::INVALID, error_msg);
    }
  }

  void executeRaw(const std::string_view statement) {
    check_connection_not_closed();
    if (mysql_query(conn, statement.data())) {
      std::string error_msg = mysql_error(conn);
    }
  }

  std::unique_ptr<Result> execute(const std::string_view statement) {
    check_connection_not_closed();
    check_error(mysql_query(conn, statement.data()));

    auto result = mysql_store_result(conn);
    if (!result) {
      check_error(1);
    }
    return std::make_unique<Result>(result);
  }

  void close() {
    if (conn) {
      mysql_close(conn);
      conn = nullptr;
    }
  }

  ~Pimpl() {
    close();
  }
};

Connection::Connection(const Credential& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(std::get<CredentialPassword>(credential), engine)) {
}

Connection::~Connection() {
}

void Connection::execute(std::string_view statement) {
  impl_->executeRaw(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  return impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  return fetchAll(statement);
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
}

void Connection::close() {
  impl_->close();
  ConnectionBase::close();
}
}