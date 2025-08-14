#pragma once


namespace sql::explain {
enum class NodeType {
  JOIN,
  GROUP_BY,
  SORT,
  SELECT,
  SCAN,
  SCAN_EMPTY,
  UNION,
  PROJECTION,
  SELECTION,
  DISTRIBUTE,
  LIMIT,
  UNKNOWN
};
}  // namespace sql::explain