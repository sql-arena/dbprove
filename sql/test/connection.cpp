#define CATCH_CONFIG_MAIN

#include <catch2/catch_test_macros.hpp>

#include "credential.h"
#include "utopia/connection.h"
#include "postgres/connection.h"
#include "connection_factory.h"
#include "sql_type.h"
/**
 * For testing actual connectivity, we rely on there being standard configuration, currently assumed to be on
 * localhost  Since we don't care about security for this usecase
 * just construct connection with hardcoded password that are set up as part of the test case
 * TODO: Change the host location to be a parameter as we will eventually want to have these things be remote.
 * @return Factories useable for testing
 */
std::vector<sql::ConnectionFactory>& factories() {
  static std::vector<sql::ConnectionFactory> factories = {
      sql::ConnectionFactory(sql::Engine("Utopia"),
                             sql::CredentialNone()),
    sql::ConnectionFactory(sql::Engine("Postgres"),
                             sql::CredentialPassword("localhost",
                                                     "postgres",
                                                     5432,
                                                     "postgres",
                                                     "password"))
      /*,
      sql::ConnectionFactory(sql::Engine("DuckDb"),
        sql::CredentialFile("test.duck"))
        */
  };
  return factories;
}

TEST_CASE("Connectivity Works", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    CHECK_NOTHROW(connection->execute(";"));
  }
}

TEST_CASE("Fetch Row", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    const auto row = connection->fetchRow("/* test_row */ SELECT 1 AS i, 'abc' AS s, CAST(0.42 AS DOUBLE) AS d");
    CHECK(row->asSqlType<sql::SqlInt>(0).get() == 1);
    CHECK(row->asSqlType<sql::SqlText>(1).get() == "abc");
    CHECK(row->asSqlType<sql::SqlDouble>(2).get() == 0.42);
    CHECK(row->asDouble(2) == 0.42);
  }
}

TEST_CASE("Fetch Scalar", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    auto v = connection->fetchValue("/* test_scalar */ SELECT 1 AS i");
  }
}

TEST_CASE("Fetch Result", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    auto r = connection->fetchAll("/* test_result */ SELECT 1 AS i, 'a' AS s UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'");
  }
}
