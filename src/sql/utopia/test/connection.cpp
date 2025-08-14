#define CATCH_CONFIG_MAIN
#include "connection.h"
#include <dbprove/sql/sql.h>
#include <catch2/catch_test_macros.hpp>



TEST_CASE("Connectivity Works", "[Connection]") {
  sql::utopia::Connection connection;
  CHECK_NOTHROW(connection.execute(";"));
}