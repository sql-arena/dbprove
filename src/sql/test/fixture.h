#pragma once
#include <string_view>
#include <vector>
#include <dbprove/sql/sql.h>

/**
* For testing actual connectivity, we rely on there being standard configuration, currently assumed to be on
 * localhost  Since we don't care about security for this usecase
 * just construct connection with hardcoded password that are set up as part of the test case
 * TODO: Change the host location to be a parameter as we will eventually want to have these things be remote.
 * @return Factories useable for testing
 */
inline std::vector<sql::ConnectionFactory> factories(bool local_only = true, const std::string_view find = "") {
  static sql::ConnectionFactory postgres_factory(
      sql::Engine("Postgres"),
      sql::CredentialPassword("localhost", "postgres", 5432, "postgres", "password"));

  static sql::ConnectionFactory duckdb_factory(
      sql::Engine("DuckDB"),
      sql::CredentialFile("C:/temp/quick.duckdb"));

  static sql::ConnectionFactory databricks_factory(sql::Engine("Databricks"),
                                                   sql::CredentialAccessToken(sql::Engine("Databricks")));

  static sql::ConnectionFactory mssql_factory(sql::Engine("SQL Server"),
                                              sql::CredentialPassword("localhost",
                                                                      "tempdb", 1433,
                                                                      "sa",
                                                                      "password"));

  static sql::ConnectionFactory mariadb_factory(sql::Engine("MariaDb"),
                                                sql::CredentialPassword("localhost", "mysql", 3306, "root",
                                                                        "password"));

  static sql::ConnectionFactory sqlite_factory(sql::Engine("Sqlite"),
                                               sql::CredentialNone());

  static std::vector local_factories = {
      postgres_factory,
      duckdb_factory,
      mssql_factory,
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

