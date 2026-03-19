#include <algorithm>
#include <dbprove/sql/sql.h>
#include <catch2/catch_test_macros.hpp>

namespace {
bool hasBalancedParentheses(const std::string& expression) {
  int depth = 0;
  for (const char c : expression) {
    if (c == '(') {
      ++depth;
    } else if (c == ')') {
      --depth;
      if (depth < 0) {
        return false;
      }
    }
  }
  return depth == 0;
}
}

TEST_CASE("Clean Expression Handles PostgreSQL Extract Syntax", "[expression]") {
  const auto tokens = sql::tokenize("EXTRACT(year FROM lineitem.l_shipdate)");
  const auto has_extract_function = std::any_of(tokens.begin(), tokens.end(), [](const sql::Token& token) {
    return token.type == sql::Token::Type::ExtractFunction;
  });
  CHECK_FALSE(has_extract_function);

  const auto q07_style = sql::cleanExpression("(EXTRACT(year FROM lineitem.l_shipdate))");
  const auto q08_style = sql::cleanExpression("(EXTRACT(year FROM orders.o_orderdate))");

  CHECK(q07_style.find("EXTRACT(EXTRACT(") == std::string::npos);
  CHECK(q08_style.find("EXTRACT(EXTRACT(") == std::string::npos);
  CHECK(hasBalancedParentheses(q07_style));
  CHECK(hasBalancedParentheses(q08_style));

  CHECK(sql::cleanExpression(q07_style) == q07_style);
  CHECK(sql::cleanExpression(q08_style) == q08_style);
}

TEST_CASE("Clean Expression Adds Spaces Around CASE Keywords", "[expression]") {
  const std::string expression =
    "SUM(CASE WHEN(p_type LIKE 'PROMO%') THEN(l_extendedprice * ('1' - l_discount)) ELSE'0'END)";
  const auto cleaned = sql::cleanExpression(expression);

  CHECK(cleaned.find("WHEN(") == std::string::npos);
  CHECK(cleaned.find("THEN(") == std::string::npos);
  CHECK(cleaned.find("ELSE'") == std::string::npos);
  CHECK(cleaned.find("'0'END") == std::string::npos);
}

TEST_CASE("Clean Expression Avoids Double Spaces In CASE Branches", "[expression]") {
  const std::string expression = "CASE WHEN Expr1013 = 0 THEN NULL ELSE Expr1014 END AS Expr1006";
  const auto cleaned = sql::cleanExpression(expression);

  CHECK(cleaned.find("WHEN  ") == std::string::npos);
  CHECK(cleaned.find("THEN  ") == std::string::npos);
  CHECK(cleaned.find("ELSE  ") == std::string::npos);
  CHECK(cleaned.find("ENDAS") == std::string::npos);
  CHECK(cleaned.find("END AS") != std::string::npos);
}

TEST_CASE("Clean Expression Strips PG Array Item Double Quotes In IN Lists", "[expression]") {
  const std::string expression =
    "((part.p_container)::text = ANY ('{\"SM CASE\",\"SM BOX\",\"SM PACK\",\"SM PKG\"}'::text[]))";
  const auto cleaned = sql::cleanExpression(expression);

  CHECK(cleaned.find("\"SM CASE\"") == std::string::npos);
  CHECK(cleaned.find("'SM CASE'") != std::string::npos);
  CHECK(cleaned.find("IN(") != std::string::npos);
}

TEST_CASE("Clean Expression Keeps Parentheses For Partial Count", "[expression]") {
  const auto cleaned = sql::cleanExpression("PARTIAL count(*)");
  CHECK(cleaned.find("PARTIAL COUNT(*)") != std::string::npos);
  CHECK(cleaned.find("COUNT *") == std::string::npos);
}

TEST_CASE("Clean Expression Handles DuckDB Scalar Subquery Error Wrapper", "[expression]") {
  const std::string expression =
    R"(CASE  WHEN ((#1 > 1)) THEN ("error"('More than one row returned by a subquery used as an expression - scalar subqueries can only return a single row. Use "SET scalar_subquery_error_on_multiple_rows=false" to revert to previous behavior of returning a random row.')) ELSE #0 END)";
  const auto cleaned = sql::cleanExpression(expression);

  CHECK(hasBalancedParentheses(cleaned));
  CHECK(cleaned.find("CASE WHEN") != std::string::npos);
  CHECK(cleaned.find("\"error\"") != std::string::npos);
  CHECK(cleaned.find("scalar_subquery_error_on_multiple_rows=false") != std::string::npos);
}
