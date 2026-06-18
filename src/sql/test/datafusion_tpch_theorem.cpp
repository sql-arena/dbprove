#include <catch2/catch_test_macros.hpp>

#include <dbprove/generator/generator_state.h>
#include <dbprove/sql/connection_factory.h>
#include <dbprove/sql/credential.h>
#include <dbprove/sql/engine.h>
#include <dbprove/theorem/theorem.h>

#include <filesystem>
#include <sstream>

TEST_CASE("DataFusion parses all TPCH theorem plans", "[DataFusion][Theorem][TPCH]") {
  dbprove::theorem::init();

  const sql::Engine engine(sql::Engine::Type::DataFusion);
  generator::GeneratorState generator(
    engine,
    std::filesystem::temp_directory_path() / "dbprove-datafusion-theorem");

  std::ostringstream console;
  dbprove::theorem::RunCtx ctx(
    engine,
    sql::CredentialNone("datafusion"),
    generator,
    console,
    "test");

  const auto theorems = dbprove::theorem::parse({"TPC-H"});

  REQUIRE(theorems.size() == 22);
  REQUIRE_NOTHROW(dbprove::theorem::prove(theorems, ctx));
  REQUIRE(ctx.proofs.size() == 22);
}
