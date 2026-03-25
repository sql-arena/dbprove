#include "join.h"

#include "glyphs.h"
#include "sql_exceptions.h"

#include <algorithm>

namespace sql::explain
{
    Join::Join(const Type type, const Strategy join_strategy, const std::string& condition,
               const EngineDialect* dialect)
        : Node(NodeType::JOIN)
        , strategy(join_strategy)
        , type(type)
        , condition(cleanExpression(condition, dialect))
        , synthetic_condition(this->condition)
    {
    }

    void Join::setSyntheticCondition(const std::string& value, const EngineDialect* dialect) {
      synthetic_condition = cleanExpression(value, dialect);
    }

    std::string Join::conditionForSql(const bool use_synthetic) const {
      if (use_synthetic && !synthetic_condition.empty()) {
        return synthetic_condition;
      }
      return condition;
    }

    bool Join::isSemiOrAnti() const {
      return type == Type::LEFT_SEMI_INNER ||
             type == Type::LEFT_SEMI_OUTER ||
             type == Type::RIGHT_SEMI_INNER ||
             type == Type::RIGHT_SEMI_OUTER ||
             type == Type::LEFT_ANTI ||
             type == Type::RIGHT_ANTI;
    }

    std::string Join::compactSymbolic() const
    {
        static const std::map<Type, const char*> join_symbols = {
            {Type::INNER, "⋈"},
            {Type::LEFT_OUTER, "⟕"},
            {Type::RIGHT_OUTER, "⟖"},
            {Type::FULL, "⟗"},
            {Type::CROSS, "×"}
        };
        static const std::map<Strategy, std::string_view> strategy_name = {
            {Strategy::HASH, "hash"},
            {Strategy::LOOP, "loop"},
            {Strategy::MERGE, "merge"}
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

    std::string Join::renderMuggle(size_t max_width) const
    {
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
            {Strategy::HASH, "HASH"},
            {Strategy::LOOP, "LOOP"},
            {Strategy::MERGE, "MERGE"}
        };
        std::string result = join_name.at(type);
        result += " JOIN ";
        result += strategy_name.at(strategy);
        if (type != Type::CROSS) {
          result += " ON ";
        }
        max_width -= result.size();
        result += ellipsify(conditionForSql(true), max_width);
        return result;
    }

    const Node& Join::buildChild() const
    {
        if (childCount() < 2) {
            throw ExplainException(
                "Join nodes must have 2 children. Did you call buildChild before parsing the full plan?");
        }
        return *firstChild();
    }

    Join::Type Join::typeFromString(const std::string_view type_string)
    {
        const std::string type_lower = to_lower(type_string);
        static const std::map<std::string_view, Join::Type> type_map = {
            {"inner", Type::INNER},
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
            {"left anti", Type::LEFT_ANTI},
            {"anti", Type::LEFT_ANTI},
            {"right outer", Type::RIGHT_OUTER},
            {"right_semi", Type::RIGHT_SEMI_INNER},
            {"right semi", Type::RIGHT_SEMI_INNER},
            {"left_semi", Type::LEFT_SEMI_INNER},
            {"semi right inner", Type::RIGHT_SEMI_INNER},
            {"semi right outer", Type::RIGHT_SEMI_OUTER},
            {"semi left inner", Type::LEFT_SEMI_INNER},
            {"semi left outer", Type::LEFT_SEMI_OUTER},
            {"semi", Type::LEFT_SEMI_INNER},
            {"full", Type::FULL}
        };

        if (!type_map.contains(type_lower)) {
            throw std::runtime_error("Join type '" + std::string(type_lower) + "' could not be mapped");
        }
        return type_map.at(type_lower);
    }


    std::string type_to_sql(Join::Type type)
    {
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

std::string renderJoinTreeSql(const Join& join, const size_t indent) {
    auto make_newline = [](const size_t i) {
      std::string out = "\n";
      for (size_t p = 0; p < i; ++p) {
        out += "  ";
      }
      return out;
    };
    std::string result = make_newline(indent);
    result += "(SELECT * ";
    result += make_newline(indent);
    auto* outer_child = join.lastChild();
    auto* inner_child = join.firstChild();
    const auto is_right_semi_or_anti =
        join.type == Join::Type::RIGHT_SEMI_INNER ||
        join.type == Join::Type::RIGHT_SEMI_OUTER ||
        join.type == Join::Type::RIGHT_ANTI;
    if (is_right_semi_or_anti) {
      // RIGHT SEMI/ANTI returns rows from the right side.
      outer_child = join.firstChild();
      inner_child = join.lastChild();
    }
    result += "FROM " + outer_child->treeSQL(indent + 1);

    const auto& fixed_condition = join.conditionForSql(true);
    switch (join.type) {
        case Join::Type::LEFT_SEMI_OUTER:
        case Join::Type::RIGHT_SEMI_OUTER: {
            std::vector<std::string> synthetic_outputs;
            for (const auto& output : join.columns_output) {
              const auto found = std::ranges::find(outer_child->columns_output, output);
              if (found == outer_child->columns_output.end()) {
                synthetic_outputs.push_back(output);
              }
            }
            if (synthetic_outputs.empty()) {
              throw ExplainException("Semi outer join expected at least one synthetic output column");
            }

            result = make_newline(indent);
            result += "(SELECT *";
            for (const auto& output : synthetic_outputs) {
              result += ", EXISTS (SELECT 1 FROM";
              result += inner_child->treeSQL(indent + 1);
              result += make_newline(indent);
              result += "WHERE " + fixed_condition;
              result += make_newline(indent);
              result += "LIMIT 1";
              result += ") AS " + output;
            }
            result += make_newline(indent);
            result += "FROM " + outer_child->treeSQL(indent + 1);
            break;
        }
        case Join::Type::LEFT_SEMI_INNER:
        case Join::Type::RIGHT_SEMI_INNER: {
            result += make_newline(indent);
            result += "WHERE EXISTS (SELECT 1 FROM";
            result += inner_child->treeSQL(indent + 1);
            result += make_newline(indent);
            result += "WHERE " + fixed_condition;
            result += make_newline(indent);
            result += "LIMIT 1";
            result += ")";
            break;
        }
        case Join::Type::LEFT_ANTI:
        case Join::Type::RIGHT_ANTI: {
            result += make_newline(indent);
            result += "WHERE NOT EXISTS (SELECT 1 FROM";
            result += inner_child->treeSQL(indent + 1);
            result += make_newline(indent);
            result += "WHERE " + fixed_condition;
            result += make_newline(indent);
            result += "LIMIT 1";
            result += ")";
            break;
        }
        default: {
            result += make_newline(indent);
            result += type_to_sql(join.type) + " " + join.firstChild()->treeSQL(indent + 1);
            if (join.type != Join::Type::CROSS) {
              result += make_newline(indent);
              result += "  ON " + fixed_condition;
              result += make_newline(indent);
            } else {
              result += make_newline(indent);
            }
            break;
        }
        }
        result += ") AS " + join.subquerySQLAlias();
        return result;
    }

std::string Join::treeSQLImpl(const size_t indent) const {
  return renderJoinTreeSql(*this, indent);
}
}
