#define CATCH_CONFIG_MAIN

#include <catch2/catch_test_macros.hpp>

#include "Credential.h"
#include "utopia/connection.h"

sql::Credential credentials = sql::Credential("test", 0, "test", "test");

TEST_CASE("Connectivity Works", "[Connection]") {
  utopia::Connection connection(credentials);
  CHECK_NOTHROW(connection.execute(";"));
}