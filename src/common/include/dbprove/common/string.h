#pragma once
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <format>


#include <regex>

template<typename Container>
std::string join(const Container& strings, const std::string& delimiter) {
    if (strings.empty()) {
        return "";
    }

    std::string result;
    result.append(strings[0]);
    for (size_t i = 1; i < strings.size(); ++i) {
        result.append(delimiter);
        result.append(strings[i]);
    }

    return result;
}


inline std::string to_lower(const std::string_view sv) {
    std::string s(sv);
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s;
}

inline std::string to_upper(const std::string_view sv) {
  std::string s(sv);
  std::transform(s.begin(), s.end(), s.begin(), ::toupper);
  return s;
}

inline std::string trim_string(const std::string_view sv) {
  const auto start = sv.find_first_not_of(" \t\n\r");
  if (start == std::string_view::npos) {
    return "";
  }
  const auto end = sv.find_last_not_of(" \t\n\r");
  return std::string(sv.substr(start, end - start + 1));
}

inline std::string strip_enclosing(const std::string_view sv, const char open, const char close) {
  auto trimmed = trim_string(sv);
  if (trimmed.size() >= 2 && trimmed.front() == open && trimmed.back() == close) {
    trimmed = trimmed.substr(1, trimmed.size() - 2);
  }
  return trimmed;
}

inline std::vector<std::string> split_top_level_by_delimiter(const std::string_view text, const char delimiter) {
  std::vector<std::string> out;
  int paren_depth = 0;
  int bracket_depth = 0;
  size_t start = 0;

  for (size_t i = 0; i < text.size(); ++i) {
    switch (text[i]) {
      case '(':
        ++paren_depth;
        continue;
      case ')':
        --paren_depth;
        continue;
      case '[':
        ++bracket_depth;
        continue;
      case ']':
        --bracket_depth;
        continue;
      default:
        break;
    }

    if (text[i] == delimiter && paren_depth == 0 && bracket_depth == 0) {
      out.push_back(trim_string(text.substr(start, i - start)));
      start = i + 1;
    }
  }

  out.push_back(trim_string(text.substr(start)));
  return out;
}


template<typename T>
inline std::u8string to_u8string(const T s) {
  return std::u8string(reinterpret_cast<const char8_t*>(s.data()), s.size());
}


inline std::string to_date_string(std::chrono::system_clock::time_point tp) {
  return std::format("{:%Y-%m-%d}", tp);
}


inline std::string mask_connection_string(const std::string_view connection_string) {
  std::string result(connection_string);
  // Match PWD= followed by anything up to a semicolon or end of string
  // Use case-insensitive matching for PWD
  std::regex pwd_regex(R"((PWD=)([^;]*))", std::regex_constants::icase);
  return std::regex_replace(result, pwd_regex, "$1*******");
}
