#pragma once


namespace sql::explain {
enum class NodeType {
  JOIN,
  GROUP_BY,
  SORT,
  SELECT,
  SCAN,
  UNION,
  PROJECTION,
  DISTRIBUTE,
  LIMIT,
  UNKNOWN
};
}  // namespace sql::explain