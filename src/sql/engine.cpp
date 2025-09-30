#include "engine.h"

#include "credential.h"
#include <dbprove/common/config.h>

namespace sql {
/// @brief Some database engine dont have a concept, so we just return this
static const std::string kDummyValue = "dummy";


Engine::Engine(const std::string_view name) {
  const static std::map<std::string_view, Type> known_names = {
      {"mariadb", Type::MariaDB},
      {"mysql", Type::MariaDB},
      {"postgresql", Type::Postgres},
      {"postgres", Type::Postgres},
      {"azurefabricwarehouse", Type::SQLServer},
      {"pg", Type::Postgres},
      {"sqlite", Type::SQLite},
      {"sqlserver", Type::SQLServer},
      {"sql server", Type::SQLServer},
      {"duckdb", Type::DuckDB},
      {"duck", Type::DuckDB},
      {"utopia", Type::Utopia},
      {"databricks", Type::Databricks},
      {"utopia", Type::Utopia},
      {"yellowbrick", Type::Yellowbrick},
      {"yb", Type::Yellowbrick},
      {"ybd", Type::Yellowbrick},
      {"clickhouse", Type::ClickHouse},
      {"ch", Type::ClickHouse}
  };

  const std::string name_lower = to_lower(name);
  if (!known_names.contains(name_lower)) {
    throw std::runtime_error("Engine '" + std::string(name) + "' not found");
  }
  type_ = known_names.at(name_lower);
}


std::string Engine::defaultDatabase(std::optional<std::string> database) const {
  if (database.has_value()) {
    return database.value();
  }
  switch (type()) {
    case Type::Databricks: {
      auto warehouse_id = getEnvVar("DATABRICKS_WAREHOUSE_ID");
      if (warehouse_id.has_value()) {
        return warehouse_id.value();
      }
      break;
    }
    case Type::Yellowbrick: {
      const auto yb_database = getEnvVar("YBDATABASE");
      if (yb_database.has_value()) {
        return yb_database.value();
      }
      return "yellowbrick";
    }
    case Type::DuckDB:
      return "duck.db";
    case Type::Postgres: {
      return "postgres";
    case Type::ClickHouse:
      return "default";
    }
  }
  throw std::invalid_argument("No default database found");
}

std::string Engine::defaultHost(std::optional<std::string> host) const {
  if (host.has_value()) {
    return host.value();
  }
  switch (type()) {
    case Type::Databricks: {
      auto env_host = getEnvVar("DATABRICKS_HOST");
      if (env_host.has_value()) {
        if (env_host.value().ends_with("/")) {
          env_host = env_host.value().substr(0, env_host.value().size() - 1);
        }
        host = env_host.value() + "/api/2.0/sql/statements";
      }
      break;
    }
    case Type::Postgres: {
      host = getEnvVar("PGHOST").value_or("localhost");
      break;
    }
    case Type::Yellowbrick:
      host = getEnvVar("YBHOST").value_or("localhost");
      break;
    case Type::ClickHouse:
    case Type::DuckDB:
      return "localhost";
    default:
      host = getEnvVar("BASE_URL",
                       "API_URL",
                       "ENDPOINT",
                       "SERVICE_URL",
                       "API_HOST");
      break;
  }
  if (host.has_value()) {
    return host.value();
  }
  throw std::invalid_argument("No default host or endpoint found");
}

uint16_t Engine::defaultPort(const uint16_t port) const {
  if (port > 0) {
    return port;
  }
  switch (type()) {
    case Type::Postgres:
      return 5432;
    case Type::Yellowbrick: {
      const auto yb_port = getEnvVar("YBPORT").value_or("5432");
      return std::stoi(yb_port);
    }
    case Type::SQLServer:
      return 1433;
    case Type::Databricks:
      return 443;
    case Type::Oracle:
      return 1521;
    case Type::ClickHouse:
      return 9000; // ClickHouse only speaks its native protocol via C++
    case Type::DuckDB:
      return 42; // Dummy port, Duck is localhost
    default:
      return 0;
  }
}


std::string Engine::defaultUsername(std::optional<std::string> username) const {
  switch (type()) {
    case Type::Postgres:
      username = getEnvVar("PGUSER").value_or("postgres");
      break;
    case Type::Yellowbrick: {
      username = getEnvVar("YBUSER").value_or("yellowbrick");
      return username.value();
    }
    case Type::ClickHouse:
      return getEnvVar("CLICKHOUSE_USER").value_or("default");
    case Type::DuckDB:
      return "";
  }
  return username.value_or("");
}

std::string Engine::defaultPassword(std::optional<std::string> password) const {
  if (password.has_value()) {
    return password.value();
  }
  switch (type()) {
    case Type::Yellowbrick:
      password = getEnvVar("YBPASSWORD").value_or("yellowbrick");
      break;
    case Type::ClickHouse:
      password = getEnvVar("CLICKHOUSE_PASSWORD").value_or("default");
    default:
      break;
  }
  return password.value_or("");
}

std::string Engine::defaultToken(std::optional<std::string> token) const {
  if (token.has_value()) {
    return token.value();
  }

  switch (type()) {
    case Type::Databricks:
      token = getEnvVar("DATABRICKS_TOKEN");
      break;
    default:
      token = getEnvVar("TOKEN", "API_TOKEN", "API_KEY", "API_SECRET");
      break;
  }
  if (token.has_value()) {
    return token.value();
  }
  return token.value_or("");
}


Credential Engine::parseCredentials(
    const std::string& host,
    const uint16_t port,
    const std::string& database,
    const std::optional<std::string>& username,
    const std::optional<std::string>& password,
    const std::optional<std::string>& token) const {
  const auto engine_name = name();

  switch (type()) {
    case Type::MariaDB:
    case Type::Postgres:
    case Type::SQLServer:
    case Type::ClickHouse:
    case Type::Yellowbrick:
    case Type::Oracle: {
      if (!username) {
        throw std::invalid_argument("Username is required for " + engine_name);
      }
      return sql::CredentialPassword(host, database, port, username.value(), password);
    }
    case Type::Utopia:
      return sql::CredentialNone();
    case Type::DuckDB:
      return sql::CredentialFile(database);
    case Type::Databricks: {
      if (!token) {
        throw std::invalid_argument("Token is required for " + engine_name);
      }
      return sql::CredentialAccessToken(*this, host, database, token.value());
    }
  }
  throw std::invalid_argument("Cannot generate credentials for engine: " + engine_name);
}

std::string Engine::name() const {
  const static std::map<Type, std::string_view> canonical_names = {
      {Type::MariaDB, "MySQL"},
      {Type::Postgres, "PostgreSQL"},
      {Type::ClickHouse, "ClickHouse"},
      {Type::SQLite, "SQLite"},
      {Type::Utopia, "Utopia"},
      {Type::DuckDB, "DuckDB"},
      {Type::Databricks, "Databricks"},
      {Type::Yellowbrick, "Yellowbrick"},
      {Type::SQLServer, "SQL Server"}
  };
  if (!canonical_names.contains(type())) {
    throw std::invalid_argument("Could not map the type to its canonical name. Are you missing a map entry?");
  }
  return std::string(canonical_names.at(type()));
}
}