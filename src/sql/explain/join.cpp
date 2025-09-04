#include "join.h"

#include "include/dbprove/sql/explain/glyphs.h"

namespace sql::explain {
std::string Join::compactSymbolic() const {
  static const std::map<Type, const char*> join_symbols = {
      {Type::INNER, "⋈"},
      {Type::LEFT_OUTER, "⟕"},
      {Type::RIGHT_OUTER, "⟖"},
      {Type::FULL, "⟗"},
      {Type::CROSS, "×"}
  };
  static const std::map<Strategy, std::string_view> strategy_name = {
      {Strategy::HASH, "hash"}, {Strategy::LOOP, "loop"}, {Strategy::MERGE, "merge"}
  };

  std::string result;
  result += join_symbols.at(type);
  result += "(";
  result += strategy_name.at(strategy);
  result += ")";
  result += "{";
  result += condition;
  result += "}";

  return result;
}

std::string Join::renderMuggle(size_t max_width) const {
  static const std::map<Type, const char*> join_name = {
      {Type::INNER, "INNER"},
      {Type::LEFT_SEMI_INNER, "LEFT SEMI"},
      {Type::LEFT_SEMI_OUTER, "LEFT SEMI"},
      {Type::LEFT_OUTER, "LEFT OUTER"},
      {Type::LEFT_ANTI, "LEFT ANTI"},
      {Type::RIGHT_OUTER, "RIGHT OUTER"},
      {Type::RIGHT_SEMI_INNER, "RIGHT SEMI INNER"},
      {Type::RIGHT_SEMI_OUTER, "RIGHT SEMI OUTER"},
      {Type::RIGHT_ANTI, "RIGHT ANTI"},
      {Type::FULL, "FULL"},
      {Type::CROSS, "CROSS"}
  };
  static const std::map<Strategy, std::string_view> strategy_name = {
      {Strategy::HASH, "HASH"}, {Strategy::LOOP, "LOOP"}, {Strategy::MERGE, "MERGE"}
  };
  std::string result = join_name.at(type);
  result += " JOIN ";
  result += strategy_name.at(strategy);
  result += " ON ";
  max_width -= result.size();
  result += ellipsify(condition, max_width);
  return result;
}
}