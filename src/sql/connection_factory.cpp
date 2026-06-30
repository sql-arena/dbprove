#include "connection_factory.h"
#include "duckdb/connection.h"
#ifndef DBPROVE_DUCKDB_ONLY
#include "cedardb/connection.h"
#include "clickhouse/connection.h"
#include "datafusion/connection.h"
#include "databricks/connection.h"
#include "mariadb/connection.h"
#include "mssql/connection.h"
#include "postgresql/connection.h"
#include "trino/connection.h"
#include "sqlite/connection.h"
#include "utopia/connection.h"
#include "yellowbrick/connection.h"
#endif

namespace sql {
std::unique_ptr<ConnectionBase> ConnectionFactory::create() {
  connectionCount_.fetch_add(1);
  const auto type = engine_.type();
  switch (type) {
#ifndef DBPROVE_DUCKDB_ONLY
    case Engine::Type::Utopia:
      return std::make_unique<utopia::Connection>(credential_, engine_, artifacts_path_);
    case Engine::Type::Postgres:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("Postgres engine requires a password credential");
      }
      return std::make_unique<postgresql::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
    case Engine::Type::CedarDB:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("CedarDB engine requires a password credential");
      }
      return std::make_unique<cedardb::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
    case Engine::Type::Yellowbrick:
      // TODO: Support other forms of authentication
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("Yellowbrick engine requires a password credential");
      }
      return std::make_unique<yellowbrick::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
    case Engine::Type::SQLServer:
      return std::make_unique<sql::mssql::Connection>(credential_, engine_, artifacts_path_);
    case Engine::Type::ClickHouse:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("ClickHouse engine requires a password credential");
      }
      return std::make_unique<clickhouse::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
#endif
    case Engine::Type::DuckDB:
      return std::make_unique<duckdb::Connection>(std::get<CredentialFile>(credential_), engine_, artifacts_path_);
#ifndef DBPROVE_DUCKDB_ONLY
    case Engine::Type::DataFusion:
      // DataFusion intentionally reuses one process-wide driver session across
      // factory.create() calls. See src/sql/datafusion/README.md.
      return std::make_unique<datafusion::Connection>(std::get<CredentialNone>(credential_), engine_, artifacts_path_);
    case Engine::Type::Databricks:
      return std::make_unique<databricks::Connection>(std::get<CredentialAccessToken>(credential_), engine_, artifacts_path_);
    case Engine::Type::MariaDB:
      return std::make_unique<mariadb::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
    case Engine::Type::SQLite:
      if (!std::holds_alternative<CredentialFile>(credential_)) {
        throw std::invalid_argument("SQLite engine requires a file credential");
      }
      return std::make_unique<sqlite::Connection>(std::get<CredentialFile>(credential_), engine_, artifacts_path_);
    case Engine::Type::Trino:
      if (!std::holds_alternative<CredentialPassword>(credential_)) {
        throw std::invalid_argument("Trino engine requires a password credential");
      }
      return std::make_unique<trino::Connection>(std::get<CredentialPassword>(credential_), engine_, artifacts_path_);
#endif
    default:
      throw std::invalid_argument("Unsupported engine type: " + engine_.name());
  }
}
}
