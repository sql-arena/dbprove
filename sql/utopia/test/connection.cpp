#define CATCH_CONFIG_MAIN

#include <catch2/catch_test_macros.hpp>

#include "Credential.h"
#include "utopia/connection.h"

TEST_CASE("Connectivity Works", "[Connection]") {
  sql::utopia::Connection connection;
  CHECK_NOTHROW(connection.execute(";"));
}