#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_ENABLE_STACK_TRACE
#include <catch2/catch_test_macros.hpp>
#include <dbprove/sql/sql.h>
#include <iostream>
#include <set>
#include <unordered_map>
#include <catch2/matchers/catch_matchers.hpp>

#include "test_connectivity/embedded_sql.h"

using namespace sql::explain;
/**
 * For testing actual connectivity, we rely on there being standard configuration, currently assumed to be on
 * localhost  Since we don't care about security for this usecase
 * just construct connection with hardcoded password that are set up as part of the test case
 * TODO: Change the host location to be a parameter as we will eventually want to have these things be remote.
 * @return Factories useable for testing
 */
auto factories(std::string_view find = "") {
  static sql::ConnectionFactory utopiaFactory(
      sql::Engine("Utopia"), sql::CredentialNone());

  static sql::ConnectionFactory postgresFactory(
      sql::Engine("Postgres"),
      sql::CredentialPassword("localhost", "postgres", 5432, "postgres", "password"));

  static sql::ConnectionFactory duckdbFactory(
      sql::Engine("DuckDB"),
      sql::CredentialFile("C:/temp/quick.duckdb"));

  static sql::ConnectionFactory databricksFactory(sql::Engine("Databricks"),
                                                  sql::CredentialAccessToken(sql::Engine("Databricks")));

  static std::vector factories = {
      utopiaFactory,
      postgresFactory,
      duckdbFactory,
      databricksFactory};

  static std::map<std::string_view, sql::ConnectionFactory*> factories_map = {
//      {"PostgreSQL", &postgresFactory},
//      {"DuckDb", &duckdbFactory},
      {"Databricks", &databricksFactory}
  };

  if (find.length() == 0) {
    return factories;
  }

  std::vector<sql::ConnectionFactory> result;
  result.push_back(*factories_map.at(find));
  return result;
}


TEST_CASE("Connectivity Works", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    CHECK_NOTHROW(connection->execute(";"));
  }
}


constexpr std::string_view kZeroRows = "SELECT 1 AS a WHERE false";
constexpr std::string_view kTwoColsZeroRow = "SELECT 1 AS a, 2 AS b WHERE false";
constexpr std::string_view kTwoColsOneRow = "SELECT 1 AS a, 2 AS b";
constexpr std::string_view kTwoRows = "SELECT 1 AS i UNION ALL SELECT 2 AS i ORDER BY i";

TEST_CASE("Fetch Row", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    const auto row = connection->fetchRow("/* test_row */ SELECT 1 AS i, 'abc' AS s, CAST(0.42 AS DOUBLE) AS d");
    CHECK(row->asSqlType<sql::SqlInt>(0).get() == 1);
    CHECK(row->asSqlType<sql::SqlString>(1).get() == "abc");
    CHECK(row->asSqlType<sql::SqlDouble>(2).get() == 0.42);
    CHECK(row->asDouble(2) == 0.42);

    if (connection->engine().type() != sql::Engine::Type::Utopia) {
      REQUIRE_THROWS_AS(connection->fetchRow(kZeroRows), sql::EmptyResultException);
      REQUIRE_THROWS_AS(connection->fetchRow(kTwoColsZeroRow), sql::EmptyResultException);
      REQUIRE_THROWS_AS(connection->fetchRow(kTwoRows), sql::InvalidRowsException);
    }
  }
}

TEST_CASE("Fetch scalar", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    auto v = connection->fetchScalar("/* test_scalar */ SELECT 1 AS i");
    CHECK(v.is<sql::SqlInt>());
    CHECK(v.get<sql::SqlInt>().get() == 1);
    if (connection->engine().type() != sql::Engine::Type::Utopia) {
      REQUIRE_THROWS_AS(connection->fetchScalar(kZeroRows), sql::EmptyResultException);
      REQUIRE_THROWS_AS(connection->fetchScalar(kTwoColsZeroRow), sql::EmptyResultException);
      REQUIRE_THROWS_AS(connection->fetchScalar(kTwoRows), sql::InvalidRowsException);
      REQUIRE_THROWS_AS(connection->fetchScalar(kTwoColsOneRow), sql::InvalidColumnsException);
    }
  }
}

TEST_CASE("Fetch Result", "[Connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    auto r = connection->fetchAll(
        "/* test_result */ SELECT 1 AS i, 'a' AS s "
        "UNION ALL SELECT 2 AS i, 'b' AS s "
        "UNION ALL SELECT 3 AS i, 'c' AS s "
        "ORDER BY i");
    CAPTURE(connection->engine().name());
    CHECK(r->rowCount() == 3);
    CHECK(r->columnCount() == 2);
    CAPTURE("Shape Correct");
    auto row_number = 0;
    for (const auto& row : r->rows()) {
      /*
          CHECK(row.asSqlType<sql::SqlInt>(0).get() == row_number + 1);
          CHECK(row.asSqlType<sql::SqlText>(1).get() == std::to_string('a' + row_number));
        */
      ++row_number;
    }
  }
}

auto make_explain(std::string_view driver = "PostgreSQL") {
  static std::set<std::string_view> did_it;
  auto pg = factories(driver);
  auto connection = pg[0].create();
  if (did_it.contains(driver)) {
    return connection;
  }
  connection->execute(resource::explain_sql);
  did_it.insert(driver);
  return connection;
}

constexpr std::string_view explain_drivers[] = {"PostgreSQL", "DuckDb"};

auto explainAndRenderPlan(const std::string_view statement) {
  for (auto& driver : explain_drivers) {
    const auto connection = make_explain(driver);
    const auto plan = connection->explain(statement);
    const auto symbolic = plan->render(Plan::RenderMode::MUGGLE);
    std::cout << symbolic << std::endl;
    CAPTURE(symbolic);
    CAPTURE(driver);
    REQUIRE(plan != nullptr);
  }
}

TEST_CASE("Explain Top N", "[Connection Explain]") {
  explainAndRenderPlan(resource::topn_sql);
}

TEST_CASE("Explain Scan", "[Connection Explain]") {
  explainAndRenderPlan("SELECT * FROM fact");
}

TEST_CASE("Explain Bushy", "[Connection Explain]") {
  explainAndRenderPlan(resource::bushy_plan_sql);
}

TEST_CASE("Explain Simple Join", "[Connection Explain]") {
  explainAndRenderPlan(resource::simple_join_sql);
}

TEST_CASE("Explain Two Join", "[Connection Explain]") {
  explainAndRenderPlan(resource::two_join_sql);
}

TEST_CASE("Explain Union All", "[Connection Explain]") {
  explainAndRenderPlan(resource::union_and_join_sql);
}