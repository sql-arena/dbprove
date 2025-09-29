#pragma once
#include "../include/dbprove/sql/explain/node.h"
#include <utility>


namespace sql::explain {
class Join final : public Node {
public:
  enum class Strategy {
    HASH, LOOP, MERGE
  };

  enum class Type {
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

  explicit Join(const Type type, const Strategy join_strategy, std::string condition)
    : Node(NodeType::JOIN)
    , strategy(join_strategy)
    , type(type)
    , condition(std::move(condition)) {
  }

  [[nodiscard]] std::string compactSymbolic() const override;

  [[nodiscard]] std::string renderMuggle(size_t max_width) const override;

  const Strategy strategy;
  const Type type;
  const std::string condition;

  /**
   * Try to figure out a type from a string.
   * Useful for parsing
   * @param type_string Any string to match against the known names
   * @return Guessed join type
   */
  static Type typeFromString(std::string_view type_string);
};
} // namespace sql::explain
