#include <catch2/catch_test_macros.hpp>
#include <dbprove/sql/sql.h>
#include "connection.h"



TEST_CASE("Empty Results iterate right", "[Result]") {
  sql::utopia::Connection connection;
  auto resultEmpty = connection.fetchAll(";");
  bool reached = false;
  for (auto& _ : resultEmpty->rows()) {
    reached = true;
  }
  REQUIRE_FALSE(reached);
}

TEST_CASE("Iterate and read rows values", "[Result]") {
  sql::utopia::Connection connection;
  auto result10 = connection.fetchAll("/* n10 */ SELECT n FROM n10;");
  int64_t actual_sum = 0;
  for (auto& row : result10->rows()) {
    int64_t v = row[0].get<sql::SqlBigInt>().get();
    actual_sum += v;
  }
  CHECK(actual_sum == 1+2+3+4+5+6+7+8+9+10);
}