#include "engine.h"

#include "connection_factory.h"
#include "credential.h"
#include <dbprove/common/config.h>
#include <dbprove/common/docker.h>
#include <dbprove/common/string.h>
#include <thread>

namespace sql {
/// @brief Some database engine dont have a concept, so we just return this
static const std::string kDummyValue = "dummy";
namespace {
constexpr uint16_t kSharedDockerHostPort = 65432;

bool commandSucceeded(const dbprove::common::DockerCommandResult& result) {
  return result.succeeded();
}

std::string firstLine(std::string output) {
  const auto newline = output.find('\n');
  if (newline != std::string::npos) {
    output.resize(newline);
  }
  return trim_string(output);
}
}


Engine::Engine(const std::string_view name) {
  const static std::map<std::string_view, Type> known_names = {{"mariadb", Type::MariaDB},
                                                               {"mysql", Type::MariaDB},
                                                               {"postgresql", Type::Postgres},
                                                               {"postgres", Type::Postgres},
                                                               {"azurefabricwarehouse", Type::SQLServer},
                                                               {"pg", Type::Postgres},
                                                               {"sqlite", Type::SQLite},
                                                               {"sqlite3", Type::SQLite},
                                                               {"mssql", Type::SQLServer},
                                                               {"sqlserver", Type::SQLServer},
                                                               {"sql server", Type::SQLServer},
                                                               {"duckdb", Type::DuckDB},
                                                               {"duck", Type::DuckDB},
                                                               {"datafusion", Type::DataFusion},
                                                               {"df", Type::DataFusion},
                                                               {"utopia", Type::Utopia},
                                                               {"databricks", Type::Databricks},
                                                               {"utopia", Type::Utopia},
                                                               {"yellowbrick", Type::Yellowbrick},
                                                               {"yb", Type::Yellowbrick},
                                                               {"ybd", Type::Yellowbrick},
                                                               {"clickhouse", Type::ClickHouse},
                                                               {"ch", Type::ClickHouse},
                                                               {"trino", Type::Trino},
                                                               {"presto", Type::Trino},
                                                               {"cedardb", Type::CedarDB},
                                                               {"cedar", Type::CedarDB}};

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
    case Type::SQLServer:
      return "dbprove";
    case Type::CedarDB:
      return "postgres";
    case Type::DuckDB:
      return "duck.db";
    case Type::DataFusion:
      return "tpch";
    case Type::SQLite:
      return "sqlite.db";
    case Type::Postgres:
      return "postgres";
    case Type::ClickHouse:
      return "default";
    case Type::Trino:
      return "tpch";
    default:
      return "";
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
        host = env_host.value();
      }
      break;
    }
    case Type::Postgres:
    case Type::CedarDB: {
      host = getEnvVar("PGHOST").value_or("localhost");
      break;
    }
    case Type::Yellowbrick:
      host = getEnvVar("YBHOST").value_or("localhost");
      break;
    case Type::ClickHouse:
    case Type::DuckDB:
    case Type::DataFusion:
    case Type::SQLite:
    case Type::SQLServer:
    case Type::Trino:
      return "localhost";
    default:
      host = getEnvVar("BASE_URL", "API_URL", "ENDPOINT", "SERVICE_URL", "API_HOST");
      break;
  }
  if (host.has_value()) {
    return host.value();
  }
  throw std::invalid_argument("No default host or endpoint found");
}

uint16_t Engine::defaultPort(const uint16_t port, const bool docker_mode) const {
  if (port > 0) {
    return port;
  }
  uint16_t default_port = 0;
  switch (type()) {
    case Type::Postgres:
    case Type::CedarDB:
      default_port = 5432;
      break;
    case Type::Yellowbrick: {
      const auto yb_port = getEnvVar("YBPORT").value_or("5432");
      default_port = std::stoi(yb_port);
      break;
    }
    case Type::SQLServer:
      default_port = 1433;
      break;
    case Type::Databricks:
      default_port = 443;
      break;
    case Type::Oracle:
      default_port = 1521;
      break;
    case Type::ClickHouse:
      default_port = 9000; // ClickHouse only speaks its native protocol via C++
      break;
    case Type::Trino:
      default_port = 8080;
      break;
    case Type::DuckDB:
    case Type::DataFusion:
    case Type::SQLite:
      default_port = 42; // Dummy port, Duck is localhost
      break;
    default:
      default_port = 0;
      break;
  }

  if (docker_mode && default_port > 0) {
    return kSharedDockerHostPort;
  }
  return default_port;
}


std::string Engine::defaultUsername(std::optional<std::string> username) const {
  switch (type()) {
    case Type::Postgres:
    case Type::CedarDB:
      username = getEnvVar("PGUSER").value_or("postgres");
      break;
    case Type::Yellowbrick: {
      username = getEnvVar("YBUSER").value_or("yellowbrick");
      return username.value();
    }
    case Type::MariaDB:
      return getEnvVar("MYSQL_USER").value_or("root");
    case Type::SQLServer:
      return "sa";
    case Type::ClickHouse:
      return getEnvVar("CLICKHOUSE_USER").value_or("default");
    case Type::Trino:
      return getEnvVar("TRINO_USER").value_or("trino");
    case Type::Utopia:
    case Type::DataFusion:
    case Type::SQLite:
      return "";
    case Type::Oracle:
    case Type::Databricks:
  default:
      // These types should have their own specialization or return empty
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
      return getEnvVar("CLICKHOUSE_PASSWORD").value_or("default");
    case Type::SQLServer:
      return "YourStrong!Passw0rd";
    case Type::MariaDB:
      password = getEnvVar("MYSQL_PWD");
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


Credential Engine::parseCredentials(const std::string& host, const uint16_t port, const std::string& database,
                                    const std::optional<std::string>& username,
                                    const std::optional<std::string>& password,
                                    const std::optional<std::string>& token) const {
  const auto engine_name = name();

  switch (type()) {
    case Type::MariaDB:
    case Type::Postgres:
    case Type::CedarDB:
    case Type::SQLServer:
    case Type::ClickHouse:
    case Type::Yellowbrick:
    case Type::Trino:
    case Type::Oracle: {
      if (!username) {
        throw std::invalid_argument("Username is required for " + engine_name);
      }
      return sql::CredentialPassword(host, database, port, username.value(), password);
    }
    case Type::Utopia:
      return sql::CredentialNone();
    case Type::DataFusion:
      return sql::CredentialNone();
    case Type::DuckDB:
    case Type::SQLite:
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

std::optional<dbprove::StorageVariant> Engine::defaultStorageVariant() const {
  switch (type()) {
    case Type::Postgres:
    case Type::CedarDB:
    case Type::SQLServer:
    case Type::ClickHouse:
      return dbprove::StorageVariant::Native;
    case Type::DataFusion:
    case Type::Trino:
      return dbprove::StorageVariant::Iceberg;
    default:
      return std::nullopt;
  }
}

std::optional<Engine::DockerServiceConfig> Engine::dockerServiceConfig(const dbprove::StorageVariant variant) const {
  switch (type()) {
    case Type::Postgres:
      if (variant == dbprove::StorageVariant::Native) {
        return DockerServiceConfig{"postgresql-native", std::chrono::seconds(60)};
      }
      return std::nullopt;
    case Type::CedarDB:
      if (variant == dbprove::StorageVariant::Native) {
        return DockerServiceConfig{"cedardb-native", std::chrono::seconds(60)};
      }
      return std::nullopt;
    case Type::SQLServer:
      if (variant == dbprove::StorageVariant::Native) {
        return DockerServiceConfig{"mssql-native", std::chrono::seconds(90)};
      }
      return std::nullopt;
    case Type::ClickHouse:
      if (variant == dbprove::StorageVariant::Native) {
        return DockerServiceConfig{"clickhouse-native", std::chrono::seconds(60)};
      }
      return std::nullopt;
    case Type::DataFusion:
      if (variant == dbprove::StorageVariant::Iceberg) {
        return DockerServiceConfig{"datafusion-iceberg", std::chrono::seconds(300)};
      }
      return std::nullopt;
    case Type::Trino:
      if (variant == dbprove::StorageVariant::Iceberg) {
        return DockerServiceConfig{"trino-iceberg", std::chrono::seconds(600)};
      }
      return std::nullopt;
    default:
      return std::nullopt;
  }
}

void Engine::waitForDockerReady(const Credential& credentials, const std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string last_error = "engine did not become ready";
  const auto default_variant = defaultStorageVariant();
  if (!default_variant.has_value()) {
    throw std::runtime_error("Engine '" + name() + "' does not define a default Docker storage variant");
  }
  const auto service_config = dockerServiceConfig(*default_variant);
  if (!service_config.has_value()) {
    throw std::runtime_error("Engine '" + name() + "' does not define a Docker service for variant '"
                             + std::string(to_string(*default_variant)) + "'");
  }
  const auto& service_name = service_config->service_name;
  dbprove::common::DockerRunner docker;

  while (std::chrono::steady_clock::now() < deadline) {
    try {
      if (type() == Type::SQLServer) {
        const auto container_id = firstLine(docker.runCompose({"ps", "-q", service_name}).output);
        if (!container_id.empty()) {
          const auto health = trim_string(docker.runDocker({
              "inspect",
              "--format",
              "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
              container_id,
          }).output);
          if (health != "healthy") {
            last_error = "SQL Server container health status is '" + health + "'";
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
          }
        }

        int stable_successes = 0;
        for (int attempt = 0; attempt < 3; ++attempt) {
          const auto probe = docker.runCompose({
              "exec", "-T", service_name,
              "/opt/mssql-tools18/bin/sqlcmd",
              "-C", "-l", "5", "-t", "5",
              "-S", "localhost",
              "-U", defaultUsername(std::nullopt),
              "-P", defaultPassword(std::nullopt),
              "-Q", "SET NOCOUNT ON; SELECT 1",
          });
          if (commandSucceeded(probe)) {
            ++stable_successes;
          } else {
            last_error = "SQL Server probe failed: " + trim_string(probe.output);
            break;
          }
          if (attempt < 2) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
        }
        if (stable_successes < 3) {
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }
      }

      if (type() == Type::Trino) {
        const auto bootstrap_marker = docker.runCompose({
            "exec", "-T", service_name, "sh", "-lc", "test -f /tmp/trino-bootstrap-ready",
        });
        if (!commandSucceeded(bootstrap_marker)) {
          last_error = "Trino bootstrap marker not present yet";
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }
      }

      if (type() == Type::Postgres || type() == Type::CedarDB) {
        const auto ready = docker.runCompose({
            "exec", "-T", service_name, "pg_isready", "-U", defaultUsername(std::nullopt),
        });
        if (!commandSucceeded(ready)) {
          last_error = "PostgreSQL/CedarDB is not ready yet";
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }
      }

      if (type() == Type::ClickHouse) {
        const auto ready = docker.runCompose({
            "exec", "-T", service_name,
            "clickhouse-client",
            "--user", defaultUsername(std::nullopt),
            "--password", defaultPassword(std::nullopt),
            "-q", "SELECT 1",
        });
        if (!commandSucceeded(ready)) {
          last_error = "ClickHouse is not ready yet";
          std::this_thread::sleep_for(std::chrono::seconds(2));
          continue;
        }
      }

      ConnectionFactory factory(*this, credentials, std::nullopt);
      auto connection = factory.create();
      connection->version();
      connection->close();
      return;
    } catch (const std::exception& e) {
      last_error = e.what();
    } catch (...) {
      last_error = "unknown non-std exception";
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  throw std::runtime_error("Timed out waiting for docker-managed engine to become ready: " + last_error);
}

std::string Engine::name() const {
  const static std::map<Type, std::string_view> canonical_names = {{Type::MariaDB, "MySQL"},
                                                                   {Type::Postgres, "PostgreSQL"},
                                                                   {Type::ClickHouse, "ClickHouse"},
                                                                   {Type::SQLite, "SQLite"},
                                                                   {Type::Utopia, "Utopia"},
                                                                   {Type::DuckDB, "DuckDB"},
                                                                   {Type::DataFusion, "DataFusion"},
                                                                   {Type::Databricks, "Databricks"},
                                                                   {Type::Yellowbrick, "Yellowbrick"},
                                                                   {Type::SQLServer, "SQL Server"},
                                                                   {Type::Trino, "Trino"},
                                                                   {Type::CedarDB, "CedarDB"}};
  if (!canonical_names.contains(type())) {
    throw std::invalid_argument("Could not map the type to its canonical name. Are you missing a map entry?");
  }
  return std::string(canonical_names.at(type()));
}

std::string Engine::internalName() const {
  switch (type_) {
    case Type::MariaDB:
      return "mariadb";
    case Type::Postgres:
      return "postgresql";
    case Type::SQLite:
      return "sqlite";
    case Type::SQLServer:
      return "mssql";
    case Type::Oracle:
      return "oracle";
    case Type::DuckDB:
      return "duckdb";
    case Type::DataFusion:
      return "datafusion";
    case Type::Utopia:
      return "utopia";
    case Type::Databricks:
      return "databricks";
    case Type::ClickHouse:
      return "clickhouse";
    case Type::Yellowbrick:
      return "yellowbrick";
    case Type::Trino:
      return "trino";
    case Type::CedarDB:
      return "cedardb";
  }
  throw std::invalid_argument("Unknown engine type");
}

bool Engine::needsLocalFile() const {
  switch (type_) {
    case Type::Databricks:
    case Type::Trino:
    case Type::DataFusion:
      return false;
    default:
      return true;
  }
}
}
