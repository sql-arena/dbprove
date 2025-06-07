#define CATCH_CONFIG_MAIN

#include <catch2/catch_test_macros.hpp>

#include "credential.h"
#include "utopia/connection.h"
#include "postgres/connection.h"
#include "connection_factory.h"

/**
 * For testing actual connectivity, we rely on there being standard configuration, currently assumed to be on
 * localhost  Since we don't care about security for this usecase
 * just construct connection with hardcoded password that are set up as part of the test case
 * TODO: Change the host location to be a parameter as we will eventually want to have these things be remote.
 * @return Factories useable for testing
 */
std::vector<sql::ConnectionFactory>& factories() {
  static std::vector<sql::ConnectionFactory> factories = {
    sql::ConnectionFactory(sql::Engine("Utopia"), sql::Credential("localhost", "test", 0, "test", "test")),
    sql::ConnectionFactory(sql::Engine("Postgres"), sql::Credential("localhost", "postgres", 5432, "postgres", "password"))
  };
  return factories;
}

TEST_CASE("Connectivity Works", "[Connection]") {
   for (auto& factory : factories()) {
    auto connection = factory.create();
    CHECK_NOTHROW(connection->execute(";"));
  }
}

