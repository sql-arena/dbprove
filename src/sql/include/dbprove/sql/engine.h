#pragma once
#include "credential.h"
#include <dbprove/common/string.h>
#include <map>


namespace sql {
/// @brief Which database engine to use
class Engine {
public:
  enum class Type {
    MariaDB,
    Postgres,
    SQLite,
    SQLServer,
    Oracle,
    DuckDB,
    Utopia,
    Databricks,
    ClickHouse,
    Yellowbrick
  };

  explicit Engine(const Type type)
    : type_(type) {
  }

  explicit Engine(const std::string_view name);

  /**
   * @brief When no endpoint or database connectivity is supplied, this is the default
   */
  std::string defaultHost(std::optional<std::string> host = std::nullopt) const;

  /**
   * @brief Default port (or 0 if no port needed) if not provided
   */
  uint16_t defaultPort(uint16_t port) const;

  /**
   * @brief Default database if none supplied
   */
  std::string defaultDatabase(std::optional<std::string> database = std::nullopt) const;
  /**
   * @brief If username is not supplied for this engine, this is the default.
   */
  std::string defaultUsername(std::optional<std::string> username = std::nullopt) const;
  /**
   * @brief If password is not supplied for this engine, this is the default.
   */
  std::string defaultPassword(std::optional<std::string> password = std::nullopt) const;
  /**
   * @brief If token is not supplied for this engine, this is the default.
   */
  std::string defaultToken(std::optional<std::string> token = std::nullopt) const;
  /// @brief  Provide entries here for friendly names of engines. These must be lowercase as we string match them
  inline static std::map<std::string_view, Type> known_names = {
      {"mariadb", Type::MariaDB},
      {"mysql", Type::MariaDB},
      {"postgresql", Type::Postgres},
      {"postgres", Type::Postgres},
      {"azurefabricwarehouse", Type::SQLServer},
      {"pg", Type::Postgres},
      {"sqlite", Type::SQLite},
      {"sqlserver", Type::SQLServer},
      {"duckdb", Type::DuckDB},
      {"duck", Type::DuckDB},
      {"utopia", Type::Utopia},
      {"databricks", Type::Databricks},
      {"utopia", Type::Utopia}
  };

  [[nodiscard]] std::string name() const;

  [[nodiscard]] Type type() const { return type_; }

  Credential parseCredentials(
      const std::string& host,
      uint16_t port,
      const std::string& database,
      const std::optional<std::string>& username,
      const std::optional<std::string>& password,
      const std::optional<std::string>& token) const;

private:
  Type type_;
};
}
