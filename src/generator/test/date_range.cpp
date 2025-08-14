#include "date_range.h"
#include "integer_range.h"

#include <array>
#include <iostream>
#include <catch2/catch_test_macros.hpp>

#include "catch2/generators/catch_generators.hpp"

using namespace std::chrono;
using namespace generator;
constexpr auto STARTDATE = sys_days(1992y / January / 1);
constexpr auto CURRENTDATE = sys_days(1995y / June / 17);
constexpr auto ENDDATE = sys_days(1998y / December / 31);

TEST_CASE("Date Range", "[Basic Generator]") {
  DateRange o_orderdate(STARTDATE, ENDDATE);

  for (int i = 0; i < 100; i++) {
    auto d = o_orderdate.next();
    REQUIRE(d >= STARTDATE);
    REQUIRE(d <= ENDDATE);
  }
}


TEST_CASE("Integer Range", "[Basic Generator]") {

  IntegerRange<> r(1,7);
  std::array<int, 7> g{};
  for (int i = 0; i < 10000; i++) {
    auto d = r.next();
    REQUIRE(d >= 1);
    REQUIRE(d <= 7);
    g[d-1]++;
  }

  for (unsigned i = 0; i < g.size(); i++) {
    CAPTURE(i);
    REQUIRE(g[i] > 0);
  }

}
