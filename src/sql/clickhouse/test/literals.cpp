#include "../literals.h"
#include <catch2/catch_test_macros.hpp>

namespace sql::clickhouse {

TEST_CASE("Strip ClickHouse typed literals normalizes numeric suffix", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("p_size IN (15_UInt16, 7_UInt8)") == "p_size IN (15, 7)");
}

TEST_CASE("Strip ClickHouse typed literals normalizes numeric suffix with parenthesized type", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("4979.819810133222_Nullable(Float64)") == "4979.819810133222");
}

TEST_CASE("Strip ClickHouse typed literals normalizes trailing-decimal numeric suffix", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("0._Float64") == "0.");
}

TEST_CASE("Strip ClickHouse typed literals normalizes numeric prefix", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("tuple(UInt64_3, Int32_-7, Float64_1.5)") == "tuple(3, -7, 1.5)");
}

TEST_CASE("Strip ClickHouse typed literals normalizes typed cast constants", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("_CAST(4979.8198 Float64, 'Float64') >= 1") == "4979.8198 >= 1");
}

TEST_CASE("Strip ClickHouse typed literals normalizes single-arg cast constants", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("c_acctbal > _CAST(4979.819810133222)") == "c_acctbal > 4979.819810133222");
}

TEST_CASE("Strip ClickHouse typed literals normalizes cast type remnant comparison", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("c_acctbal > _CAST(4979.819810133222) > 'Float64'") == "c_acctbal > 4979.819810133222");
}

TEST_CASE("Strip ClickHouse typed literals normalizes artifact cast pattern with typed suffixes", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("_CAST(4979.819810133222_Nullable(Float64), 'Nullable(Float64)'_String)") == "4979.819810133222");
}

TEST_CASE("Strip ClickHouse typed literals normalizes string prefix", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("String_'AIR'") == "'AIR'");
}

TEST_CASE("Strip ClickHouse typed literals does not rewrite regular identifiers", "[clickhouse][literals]") {
  CHECK(stripClickHouseTypedLiterals("table_1 = 1") == "table_1 = 1");
}

}
