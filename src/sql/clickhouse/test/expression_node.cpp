#include "../expression_node.h"
#include <catch2/catch_test_macros.hpp>

namespace sql::clickhouse {

namespace {
ExpressionNode makeToNullableColumnExpression() {
  ExpressionNode root;
  root.kind = ExpressionNode::Kind::FUNCTION;
  root.function_name = "toNullable";

  auto column = std::make_unique<ExpressionNode>();
  column->kind = ExpressionNode::Kind::COLUMN;
  column->source_name = "o_orderkey";
  root.addChild(std::move(column));
  return root;
}
}

TEST_CASE("ClickHouse ExpressionNode renders toNullable as a no-op", "[clickhouse][expression_node]") {
  const auto expression = makeToNullableColumnExpression();

  CHECK(expression.renderSql() == "o_orderkey");
  CHECK(expression.renderExecutableSql() == "o_orderkey");
  CHECK(expression.renderUser() == "o_orderkey");
}

} // namespace sql::clickhouse
