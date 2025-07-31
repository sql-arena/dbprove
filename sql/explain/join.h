#pragma once
#include "node.h"

namespace sql::explain {
class Join : public Node {
public:
  enum class Strategy {
    HASH, LOOP, MERGE
  };

  enum class Type {
    INNER, LEFT, RIGHT, FULL, CROSS
  };

  explicit Join(const Type type, const Strategy join_strategy, const std::string& condition)
    : Node(NodeType::JOIN)
    , strategy(join_strategy)
    , type(type)
    , condition(condition) {
  }

  std::string compactSymbolic() const override {
    static const std::map<Type, const char*> join_symbols = {
        {Type::INNER, "⋈"},
        {Type::LEFT, "⟕"},
        {Type::RIGHT, "⟖"},
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
  std::string renderMuggle() const override {
    static const std::map<Type, const char*> join_name = {
      {Type::INNER, "INNER"},
      {Type::LEFT, "LEFT"},
      {Type::RIGHT, "RIGHT"},
      {Type::FULL, "FULL"},
      {Type::CROSS, "CROSS"}
    };
    static const std::map<Strategy, std::string_view> strategy_name = {
      {Strategy::HASH, "HASH"}, {Strategy::LOOP, "LOOP"}, {Strategy::MERGE, "MERGE"}
    };
    std::string result = join_name.at(type) ;
    result += " JOIN ";
    result += strategy_name.at(strategy);
    result += " ON " + condition;
    return result;
  }
  const Strategy strategy;
  const Type type;
  const std::string condition;
};
} // namespace sql::explain