#include "engine.h"

#include <common/config.h>

namespace sql {
std::string Engine::defaultDatabase() const {
  std::optional<std::string> r;
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

std::string Engine::defaultHost() const {
  std::optional<std::string> v;
  switch (type()) {
    case Type::Databricks: {
      auto host = getEnvVar("DATABRICKS_HOST");
      if (host.has_value()) {
        if (host.value().ends_with("/")) {
          host = host.value().substr(0, host.value().size() - 1);
        }
        v = host.value() + "/api/2.0/sql/statements";
      }
      break;
    }
    default:
      v = getEnvVar("BASE_URL",
                    "API_URL",
                    "ENDPOINT",
                    "SERVICE_URL",
                    "API_HOST");
      break;
  }
  if (v.has_value()) {
    return v.value();
  }
  throw std::invalid_argument("No default host or endpoint found");
}


std::string Engine::defaultUsernameOrToken() const {
  std::optional<std::string> v;
  switch (type()) {
    case Type::Databricks:
      v = getEnvVar("DATABRICKS_TOKEN");
      break;
    default:
      v = getEnvVar("TOKEN", "API_TOKEN", "API_KEY", "API_SECRET");
      break;
  }
  if (v.has_value()) {
    return v.value();
  }
  throw std::invalid_argument("No default token or username found");
}
}