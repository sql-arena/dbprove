#include "join.h"

#include "glyphs.h"

namespace sql::explain {
Join::Join(const Type type, const Strategy join_strategy, std::string condition)
  : Node(NodeType::JOIN)
  , strategy(join_strategy)
  , type(type)
  , condition(cleanExpression(condition)) {
}

std::string Join::compactSymbolic() const {
  static const std::map<Type, const char*> join_symbols = {{Type::INNER, "⋈"},
                                                           {Type::LEFT_OUTER, "⟕"},
                                                           {Type::RIGHT_OUTER, "⟖"},
                                                           {Type::FULL, "⟗"},
                                                           {Type::CROSS, "×"}};
  static const std::map<Strategy, std::string_view> strategy_name = {{Strategy::HASH, "hash"},
                                                                     {Strategy::LOOP, "loop"},
                                                                     {Strategy::MERGE, "merge"}};

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
  static const std::map<Type, const char*> join_name = {{Type::INNER, "INNER"},
                                                        {Type::LEFT_SEMI_INNER, "LEFT SEMI"},
                                                        {Type::LEFT_SEMI_OUTER, "LEFT SEMI"},
                                                        {Type::LEFT_OUTER, "LEFT OUTER"},
                                                        {Type::LEFT_ANTI, "LEFT ANTI"},
                                                        {Type::RIGHT_OUTER, "RIGHT OUTER"},
                                                        {Type::RIGHT_SEMI_INNER, "RIGHT SEMI INNER"},
                                                        {Type::RIGHT_SEMI_OUTER, "RIGHT SEMI OUTER"},
                                                        {Type::RIGHT_ANTI, "RIGHT ANTI"},
                                                        {Type::FULL, "FULL"},
                                                        {Type::CROSS, "CROSS"}};
  static const std::map<Strategy, std::string_view> strategy_name = {{Strategy::HASH, "HASH"},
                                                                     {Strategy::LOOP, "LOOP"},
                                                                     {Strategy::MERGE, "MERGE"}};
  std::string result = join_name.at(type);
  result += " JOIN ";
  result += strategy_name.at(strategy);
  result += " ON ";
  max_width -= result.size();
  result += ellipsify(condition, max_width);
  return result;
}

Join::Type Join::typeFromString(const std::string_view type_string) {
  const std::string type_lower = to_lower(type_string);
  static const std::map<std::string_view, Join::Type> type_map = {{"inner", Type::INNER},
                                                                  {"left", Type::LEFT_OUTER},
                                                                  {"left outer", Type::LEFT_OUTER},
                                                                  {"right", Type::RIGHT_OUTER},
                                                                  {"right anti", Type::RIGHT_ANTI},
                                                                  {"anti right", Type::RIGHT_ANTI},
                                                                  {"left anti", Type::LEFT_ANTI},
                                                                  {"anti left", Type::LEFT_ANTI},
                                                                  {"right outer", Type::RIGHT_OUTER},
                                                                  {"semi right inner", Type::RIGHT_SEMI_INNER},
                                                                  {"semi right outer", Type::RIGHT_SEMI_OUTER},
                                                                  {"semi left inner", Type::LEFT_SEMI_INNER},
                                                                  {"semi left outer", Type::LEFT_SEMI_OUTER},
                                                                  {"semi", Type::LEFT_SEMI_INNER},
                                                                  {"full", Type::FULL}};

  if (!type_map.contains(type_lower)) {
    throw std::runtime_error("Join type '" + std::string(type_string) + "' could not be mapped");
  }
  return type_map.at(type_lower);
}
}