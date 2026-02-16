#include "fixture.h"


std::vector<sql::ConnectionFactory> factories(const bool local_only, const std::string_view find) {
  static auto temp_dir = std::filesystem::temp_directory_path();
  static sql::ConnectionFactory postgres_factory(
      sql::Engine("Postgres"),
      sql::CredentialPassword("localhost",
                              "postgres",
                              5432,
                              "postgres",
                              "password"));

  static sql::ConnectionFactory duckdb_factory(
      sql::Engine("DuckDB"),
      sql::CredentialFile((temp_dir / "quick.duckdb").string()));

  static sql::ConnectionFactory databricks_factory(
      sql::Engine("Databricks"),
      sql::CredentialAccessToken(sql::Engine("Databricks")));
#ifndef __APPLE__
  static sql::ConnectionFactory mssql_factory(
      sql::Engine("SQL Server"),
      sql::CredentialPassword("localhost",
                              "tempdb",
                              1433,
                              "sa",
                              "password"));
#endif
  static sql::ConnectionFactory mariadb_factory(
      sql::Engine("MariaDb"),
      sql::CredentialPassword("localhost",
                              "mysql",
                              3306,
                              "root",
                              "password"));

  static sql::ConnectionFactory sqlite_factory(
      sql::Engine("Sqlite"),
      sql::CredentialFile((temp_dir / "quick.sqlite").string()));

  static std::vector local_factories = {
      postgres_factory,
      duckdb_factory,
#ifndef __APPLE__
      mssql_factory,
#endif
      mariadb_factory,
      sqlite_factory
  };

  if (local_only) {
    return local_factories;
  }

  if (find.length() == 0) {
    return local_factories;
  }

  std::vector<sql::ConnectionFactory> result;
  for (auto& factory : local_factories) {
    if (factory.engine().name() == find) {
      result.push_back(factory);
    }
  }
  return result;
}