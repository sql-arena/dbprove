#pragma once
#include "../include/dbprove/sql/explain/node.h"

namespace sql::explain
{
  class Join : public Node
  {
  public:
    enum class Strategy
    {
      HASH, LOOP, MERGE
    };

    enum class Type
    {
      INNER,
      LEFT_OUTER,
      LEFT_SEMI_INNER,
      LEFT_SEMI_OUTER,
      LEFT_ANTI,
      RIGHT_OUTER,
      RIGHT_SEMI_INNER,
      RIGHT_SEMI_OUTER,
      RIGHT_ANTI,
      FULL,
      CROSS
    };

    explicit Join(const Type type, const Strategy join_strategy, const std::string& condition)
      : Node(NodeType::JOIN)
      , strategy(join_strategy)
      , type(type)
      , condition(condition)
    {
    }

    std::string compactSymbolic() const override;

    std::string renderMuggle(size_t max_width) const override;

    const Strategy strategy;
    const Type type;
    const std::string condition;
  };
} // namespace sql::explain
