#include "join.h"

#include <regex>

#include "glyphs.h"
#include "sql_exceptions.h"

namespace sql::explain {
Join::Join(const Type type, const Strategy join_strategy, const std::string& condition)
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

const Node& Join::buildChild() const {
  if (childCount() < 2) {
    throw ExplainException("Join nodes must have 2 children. Did you call buildChild before parsing the full plan?");
  }
  return *firstChild();
}

Join::Type Join::typeFromString(const std::string_view type_string) {
  const std::string type_lower = to_lower(type_string);
  static const std::map<std::string_view, Join::Type> type_map = {{"inner", Type::INNER},
                                                                  {"inner join", Type::INNER},
                                                                  {"right semi join", Type::RIGHT_SEMI_INNER},
                                                                  {"left semi join", Type::RIGHT_SEMI_INNER},
                                                                  {"left anti semi join", Type::LEFT_ANTI},
                                                                  {"right anti semi join", Type::LEFT_ANTI},
                                                                  {"left", Type::LEFT_OUTER},
                                                                  {"left outer", Type::LEFT_OUTER},
                                                                  {"mark", Type::INNER},
                                                                  {"right", Type::RIGHT_OUTER},
                                                                  {"right anti", Type::RIGHT_ANTI},
                                                                  {"right_anti", Type::RIGHT_ANTI},
                                                                  {"anti right", Type::RIGHT_ANTI},
                                                                  {"left anti", Type::LEFT_ANTI},
                                                                  {"anti left", Type::LEFT_ANTI},
                                                                  {"right outer", Type::RIGHT_OUTER},
                                                                  {"right_semi", Type::RIGHT_SEMI_INNER},
                                                                  {"left_semi", Type::LEFT_SEMI_INNER},
                                                                  {"semi right inner", Type::RIGHT_SEMI_INNER},
                                                                  {"semi right outer", Type::RIGHT_SEMI_OUTER},
                                                                  {"semi left inner", Type::LEFT_SEMI_INNER},
                                                                  {"semi left outer", Type::LEFT_SEMI_OUTER},
                                                                  {"semi", Type::LEFT_SEMI_INNER},
                                                                  {"full", Type::FULL}};

  if (!type_map.contains(type_lower)) {
    throw std::runtime_error("Join type '" + std::string(type_lower) + "' could not be mapped");
  }
  return type_map.at(type_lower);
}


std::string type_to_sql(Join::Type type) {
  switch (type) {
    case Join::Type::INNER:
      return "JOIN";
    case Join::Type::LEFT_OUTER:
    case Join::Type::RIGHT_OUTER:
      /* LEFT and RIGHT as algorithmic, the query is still left */
      return "LEFT JOIN";
    case Join::Type::FULL:
      return "FULL JOIN";
    case Join::Type::CROSS:
      return "CROSS JOIN";
    default:
      throw std::runtime_error("Cannot translate join type to SQL");
  }
}

/**
 * In ClickHouse, it is possible to have a join condition that looks like this:
 *
 * foo = foo
 *
 * This occurs if foo is in both left and right side of the join. This is not the trivially true equality
 * it is ClickHouse explain that is really, really odd.
 *
 * A proper EXPLAIN would keep track of aliases.
 * No such luck with ClickHouse
 *
 * HACK (filthy): Look for things of the form
 *
 *    <col> <op> <col>
 *
 * Replace wih aliases we assigned to make:
 *
 *    L.<col> <op> R.<col>
 *
 */
std::string fixCondition(const std::string& condition, const std::string& leftAlias, const std::string& rightAlias) {
  static const std::regex pattern(R"((\b\w+\b)\s*(=|<>)\s*(\b\w+\b))");
  std::string result;
  std::sregex_iterator begin(condition.begin(), condition.end(), pattern);
  std::sregex_iterator end;

  size_t last_pos = 0;
  for (auto it = begin; it != end; ++it) {
    const std::smatch& match = *it;
    result += condition.substr(last_pos, match.position() - last_pos);
    if (match[1] == match[3]) {
      result += leftAlias + "." + match[1].str() + " " + match[2].str() + " " + rightAlias + "." + match[3].str();
    } else {
      result += match.str();
    }
    last_pos = match.position() + match.length();
  }
  result += condition.substr(last_pos);
  return result;
}


std::string Join::treeSQLImpl(const size_t indent) const {
  std::string result = newline(indent);
  result += "(SELECT * ";
  result += newline(indent);
  result += "FROM " + lastChild()->treeSQL(indent + 1);

  auto fixed_condition = fixCondition(condition, firstChild()->subquerySQLAlias(), lastChild()->subquerySQLAlias());
  switch (type) {
    case Type::LEFT_SEMI_INNER:
    case Type::LEFT_SEMI_OUTER:
    case Type::RIGHT_SEMI_INNER:
    case Type::RIGHT_SEMI_OUTER: {
      result += newline(indent);
      result += "WHERE EXISTS (SELECT * FROM";
      result += firstChild()->treeSQL(indent + 1);
      result += newline(indent);
      result += "WHERE " + fixed_condition;
      result += ")";
      break;
    }
    case Type::LEFT_ANTI:
    case Type::RIGHT_ANTI: {
      result += newline(indent);
      result += "WHERE NOT EXISTS (SELECT * FROM";
      result += firstChild()->treeSQL(indent + 1);
      result += newline(indent);
      result += "WHERE " + fixed_condition;
      result += ")";
      break;
    }
    default: {
      result += newline(indent);
      result += type_to_sql(type) + " " + firstChild()->treeSQL(indent + 1);
      result += newline(indent);
      result += "  ON " + fixed_condition;
      result += newline(indent);
      break;
    }
  }
  result += ") AS " + subquerySQLAlias();
  return result;
}
}