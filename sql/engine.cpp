#include "engine.h"

#include <common/config.h>

namespace sql {
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

    case Type::Postgres: {
      return "postgres";
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
    case Type::SQLServer:
      return 1433;
    case Type::Databricks:
      return 443;
    case Type::Oracle:
      return 1521;
    case Type::ClickHouse:
      return 9000;
    default:
      return 0;
  }
}


std::string Engine::defaultUsername(std::optional<std::string> username) const {
  switch (type()) {
    case Type::Postgres:
      username = getEnvVar("PGUSER");
      if (!username) {
        username = "postgres";
      }
      break;
    case Type::Yellowbrick:
      username = getEnvVar("YB_USER");
    if (!username) {
      username = "yellowbrick";
    }
    break;
  }
  return username.value_or("");

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
}