#include "connection_factory.h"
#include "duckdb/connection.h"
#include "postgresql/connection.h"
#include "utopia/connection.h"
#ifndef __APPLE__
#include "msodbc/connection.h"
#endif
#include "databricks/connection.h"
#include "yellowbrick/connection.h"
#include "clickhouse/connection.h"
#include "mariadb/connection.h"
#include "sqlite/connection.h"

namespace sql {
std::unique_ptr<ConnectionBase> ConnectionFactory::create() {
  connectionCount_.fetch_add(1);
  const auto type = engine_.type();
  switch (type) {
    case Engine::Type::Utopia:
      return std::make_unique<utopia::Connection>();
    case Engine::Type::Postgres:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("Postgres engine requires a password credential");
      }
      return std::make_unique<postgresql::Connection>(std::get<CredentialPassword>(credential_), engine_);
    case Engine::Type::Yellowbrick:
      // TODO: Support other forms of authentication
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("Yellowbrick engine requires a password credential");
      }
      return std::make_unique<yellowbrick::Connection>(std::get<CredentialPassword>(credential_), engine_);
#ifndef __APPLE__
    case Engine::Type::SQLServer:
      return std::make_unique<msodbc::Connection>(credential_, engine_);
#endif
    case Engine::Type::ClickHouse:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("ClickHouse engine requires a password credential");
      }
      return std::make_unique<clickhouse::Connection>(std::get<CredentialPassword>(credential_), engine_);
    case Engine::Type::DuckDB:
      return std::make_unique<duckdb::Connection>(std::get<CredentialFile>(credential_), engine_);
    case Engine::Type::Databricks:
      return std::make_unique<databricks::Connection>(std::get<CredentialAccessToken>(credential_), engine_);
    case Engine::Type::MariaDB:
      return std::make_unique<mariadb::Connection>(std::get<CredentialPassword>(credential_), engine_);
    case Engine::Type::SQLite:
      if (!std::holds_alternative<CredentialFile>(credential_)) {
        throw std::invalid_argument("SQLite engine requires a file credential");
      }
      return std::make_unique<sqlite::Connection>(std::get<CredentialFile>(credential_), engine_);
    default:
      throw std::invalid_argument("Unsupported engine type: " + engine_.name());
  }
}
}