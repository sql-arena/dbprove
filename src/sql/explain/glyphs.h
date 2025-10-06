#pragma once
#include <string>
#include <string_view>

namespace sql::explain {
constexpr std::string_view kEllipsis = "...";

inline std::string ellipsify(std::string s, const size_t max_width) {
  if (s.size() <= max_width) {
    return std::move(s);
  }
  const std::string result = std::move(s);
  return result.substr(0, max_width - kEllipsis.size()) + std::string(kEllipsis);
}


template <typename Collection>
std::string join(const Collection& columns,
                 const std::string& delimiter,
                 size_t max_width = std::numeric_limits<size_t>::max(),
                 const bool with_order = false) {
  if (columns.empty()) {
    return "";
  }
  std::string result;
  auto remaining_width = max_width;
  size_t i = 0;
  do {
    std::string add_column;
    if (i > 1) {
      add_column.append(delimiter);
    }
    add_column.append(columns[i].name);
    if (with_order) {
      add_column.append(" ").append(to_string(columns[i].sorting));
    }
    result.append(ellipsify(add_column, remaining_width));
    remaining_width = max_width - result.size();
    i++;
  } while (i < columns.size() && remaining_width > kEllipsis.size() + delimiter.size());
  return result;
}
}