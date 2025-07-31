#pragma once
#include <string>
#include <vector>
#include <algorithm>


template<typename Container>
std::string join(const Container& strings, const std::string& delimiter) {
    if (strings.empty()) {
        return "";
    }

    std::string result;
    result.append(*strings[0]);
    for (size_t i = 1; i < strings.size(); ++i) {
        result.append(delimiter);
        result.append(*strings[i]);
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


template<typename T>
inline std::u8string to_u8string(const T s) {
  return std::u8string(reinterpret_cast<const char8_t*>(s.data()), s.size());
}