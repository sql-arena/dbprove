#include "fixture.h"
#include "test_connectivity/embedded_sql.h"
#include <dbprove/sql/sql.h>
#include <iostream>
#include <set>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

using namespace sql::explain;


auto make_explain(std::string_view driver = "PostgreSQL") {
  static std::set<std::string_view> did_it;
  auto pg = factories(true, driver);
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
    plan->render(std::cout, 120, Plan::RenderMode::MUGGLE);
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