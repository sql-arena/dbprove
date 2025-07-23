#pragma once


namespace sql::explain {
enum class NodeType {
  JOIN,
  GROUP_BY,
  SORT,
  SELECT,
  SCAN,
  EXPRESSION,
  DISTRIBUTE
};
}  // namespace sql::explain