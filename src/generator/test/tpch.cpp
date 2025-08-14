#include <iostream>
#include <catch2/catch_test_macros.hpp>

#include "generator_state.h"
#include "tpch/tpch_text.h"

#include "variable_integer.h"
#include "tpch/tpch.h"

#include "catch2/generators/catch_generators.hpp"
using namespace generator;


TEST_CASE("TpcH text is right length", "[TPCH]") {
  VariableInteger r;
  for (int i = 0; i < 1000; i++) {
    const auto min = r.next(1000);
    const auto max = min + r.next(1000);

    TpchText g(min, max);

    for (int j = 0; j < 10; j++) {
      std::string s = g.next();
      REQUIRE((s.length() >= min && s.length() <= max));
      std::cout << s << std::endl;
    }
  }
}

TEST_CASE("Max and min sized text", "[TPCH]") {
  TpchText zero(0, 0);
  REQUIRE(zero.next().empty());

  TpchText max(TpchText::max_generated_text_length, TpchText::max_generated_text_length);
  REQUIRE(max.next().length() == TpchText::max_generated_text_length);
}


std::unique_ptr<GeneratorState> gen_state() {
  auto tmp = std::filesystem::temp_directory_path();
  return std::make_unique<GeneratorState>(tmp);
}

TEST_CASE("Generate Tables", "[TPCH]") {

}