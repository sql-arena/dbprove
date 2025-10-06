#include "fixture.h"
#include "test_connectivity/embedded_sql.h"

#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_ENABLE_STACK_TRACE
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <iostream>
#include <set>

using namespace sql::explain;


constexpr std::string_view kZeroRows = "SELECT 1 AS a WHERE 1=0";
constexpr std::string_view kTwoColsZeroRow = "SELECT 1 AS a, 2 AS b WHERE 1=0";
constexpr std::string_view kTwoColsOneRow = "SELECT 1 AS a, 2 AS b";
constexpr std::string_view kTwoRows = "SELECT 1 AS i UNION ALL SELECT 2 AS i ORDER BY i";


TEST_CASE("Connectivity Works", "[connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    CHECK_NOTHROW(connection->execute("SELECT 1"));
  }
}

TEST_CASE("Connection Close Works", "[connection]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    connection->close();
    CAPTURE(factory.engine().name());
    REQUIRE_THROWS_AS(connection->execute("SELECT 1"), sql::ConnectionClosedException);
  }
}


TEST_CASE("Fetch Result", "[query]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    const auto r = connection->fetchAll(
        "/* test_result */ SELECT 1 AS i, 'a' AS s "
        "UNION ALL SELECT 2 AS i, 'b' AS s "
        "UNION ALL SELECT 3 AS i, 'c' AS s "
        "ORDER BY i");
    CAPTURE(connection->engine().name());
    CHECK(r->columnCount() == 2);
    CAPTURE(r->columnCount());
    auto row_number = 0;
    for (const auto& row : r->rows()) {
      CHECK(row.asSqlType<sql::SqlInt>(0).get() == row_number + 1);
      auto expected = std::string(1, 'a' + row_number);
      auto actual = row.asSqlType<sql::SqlString>(1).get();
      CHECK(actual == expected);
      ++row_number;
    }
    CHECK(row_number == 3);
    // Depending on the driver, the row count may not be known until all rows have been fetched.
    CHECK(r->rowCount() == 3);
  }
}

TEST_CASE("Fetch Row", "[query]") {
  for (auto& factory : factories()) {
    CAPTURE(factory.engine().name());
    const auto connection = factory.create();
    const auto row = connection->fetchRow("/* test_row */ SELECT 1 AS i, 'abc' AS s, CAST(0.42 AS DOUBLE) AS d");
    CHECK(row->asSqlType<sql::SqlInt>(0).get() == 1);
    CHECK(row->asSqlType<sql::SqlString>(1).get() == "abc");
    CHECK(row->asSqlType<sql::SqlDouble>(2).get() == 0.42);
    CHECK(row->asDouble(2) == 0.42);

    REQUIRE_THROWS_AS(connection->fetchRow(kZeroRows), sql::EmptyResultException);
    REQUIRE_THROWS_AS(connection->fetchRow(kTwoColsZeroRow), sql::EmptyResultException);
    REQUIRE_THROWS_AS(connection->fetchRow(kTwoRows), sql::InvalidRowsException);
  }
}

TEST_CASE("Fetch scalar", "[query]") {
  for (auto& factory : factories()) {
    CAPTURE(factory.engine().name());
    const auto connection = factory.create();
    auto v = connection->fetchScalar("/* test_scalar */ SELECT 1 AS i");
    CHECK(v.asInt8() == 1);
    REQUIRE_THROWS_AS(connection->fetchScalar(kZeroRows), sql::EmptyResultException);
    REQUIRE_THROWS_AS(connection->fetchScalar(kTwoColsZeroRow), sql::EmptyResultException);
    REQUIRE_THROWS_AS(connection->fetchScalar(kTwoRows), sql::InvalidRowsException);
    REQUIRE_THROWS_AS(connection->fetchScalar(kTwoColsOneRow), sql::InvalidColumnsException);
  }
}

TEST_CASE("Fetch Types", "[query]") {
  for (auto& factory : factories()) {
    const auto connection = factory.create();
    // connection->execute(resource::sql_type_sql);
    // auto data = connection->fetchAll("SELECT * FROM sql_types");
  }
}