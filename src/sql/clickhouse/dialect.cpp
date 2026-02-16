#include "dialect.h"
#include "expression.h"

namespace sql::clickhouse {
const std::map<std::string_view, std::string_view>& ClickHouseDialect::engineFunctions() const {
  static const std::map<std::string_view, std::string_view> m = {{"UNIQEXACT", "COUNT DISTINCT"}, {"SUMIF", ""}};
  return m;
}

const std::set<std::string_view>& ClickHouseDialect::castFunctions() const {
  static const std::set<std::string_view> s = {"_CAST"};
  return s;
}

void ClickHouseDialect::preRender(std::vector<Token>& tokens) {
  for (size_t i = 0; i < tokens.size(); ++i) {
    auto& token = tokens[i];
    if (token.type == Token::Type::Cast) {
      auto& next = tokens[i + 1];
      next.value = removeQuotes(tokens[i + 1].value);
      next.type = Token::Type::Literal;
      continue;
    }
    if (token.type != Token::Type::Function) {
      continue;
    }
    // In another extraordinary display of madness, ClickHouse requires the sumIf function to be case-sensitive
    if (token.value == "SUMIF") {
      token.value = "sumIf";
    }
  }
}
}
