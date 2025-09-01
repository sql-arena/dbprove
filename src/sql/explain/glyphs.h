#pragma once
#include <string>
#include <string_view>

namespace sql::explain {
constexpr std::string_view kEllipsis = "...";

inline std::string ellipsify(std::string s, const size_t max_width) {
  if (s.size() > max_width) {
    return std::move(s);
  }
  const std::string result = std::move(s);
  return result.substr(0, max_width - kEllipsis.size()) + std::string(kEllipsis);
}
}