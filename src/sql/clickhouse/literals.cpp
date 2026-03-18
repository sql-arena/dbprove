#include "literals.h"
#include <regex>
#include <dbprove/common/string.h>

namespace sql::clickhouse {

std::string stripClickHouseTypedLiterals(std::string input) {
  auto normalize_quoted_literal_with_trailing_parens = [](const std::string& raw) -> std::string {
    const auto text = trim_string(raw);
    if (text.size() < 2 || text.front() != '\'') {
      return raw;
    }
    for (size_t i = 1; i < text.size(); ++i) {
      if (text[i] != '\'') {
        continue;
      }
      if (i + 1 < text.size() && text[i + 1] == '\'') {
        ++i;
        continue;
      }
      const auto suffix = trim_string(text.substr(i + 1));
      if (!suffix.empty() &&
          std::all_of(suffix.begin(), suffix.end(), [](const char c) { return c == ')'; })) {
        return text.substr(0, i + 1);
      }
      return raw;
    }
    return raw;
  };

  auto unwrap_cast_literal = [](const std::string& raw) -> std::string {
    const auto text = trim_string(raw);
    const auto upper = to_upper(text);
    size_t prefix_len = 0;
    if (upper.starts_with("_CAST(")) {
      prefix_len = 6;
    } else if (upper.starts_with("CAST(")) {
      prefix_len = 5;
    } else {
      return raw;
    }
    if (text.size() <= prefix_len || text.back() != ')') {
      return raw;
    }

    const auto inside = text.substr(prefix_len, text.size() - prefix_len - 1);
    int depth = 0;
    bool in_single_quote = false;
    for (size_t i = 0; i < inside.size(); ++i) {
      const auto c = inside[i];
      if (c == '\'') {
        if (in_single_quote && i + 1 < inside.size() && inside[i + 1] == '\'') {
          ++i;
          continue;
        }
        in_single_quote = !in_single_quote;
        continue;
      }
      if (in_single_quote) {
        continue;
      }
      if (c == '(') {
        ++depth;
        continue;
      }
      if (c == ')') {
        if (depth > 0) {
          --depth;
        }
        continue;
      }
      if (c != ',' || depth != 0) {
        continue;
      }

      const auto first_arg = trim_string(inside.substr(0, i));
      if (first_arg.empty()) {
        return raw;
      }
      const auto first_upper = to_upper(first_arg);
      const bool looks_literal =
          first_arg.front() == '\'' ||
          std::isdigit(static_cast<unsigned char>(first_arg.front())) != 0 ||
          first_arg.front() == '+' ||
          first_arg.front() == '-' ||
          first_upper == "NULL" ||
          first_upper == "TRUE" ||
          first_upper == "FALSE";
      return looks_literal ? first_arg : raw;
    }
    return raw;
  };

  input = unwrap_cast_literal(input);
  static const std::string typed_prefix =
      R"((?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|Decimal(?:32|64|128|256)?(?:\([^)]*\))?|DateTime64(?:\([^)]*\))?|DateTime|Date32|Date|String|FixedString\(\d+\)|UUID|Bool|IPv[46]|Enum(?:8|16)\([^)]*\)|Nullable\([^)]*\)|LowCardinality\([^)]*\)))";

  // Some render paths can expose Const(<Type>)_... forms.
  input = std::regex_replace(
      input,
      std::regex("\\bConst\\(\\s*" + typed_prefix + R"(\s*\)_([+-]?[0-9]+(?:\.[0-9]*)?)\b)"),
      "$1");
  input = std::regex_replace(
      input,
      std::regex("\\bConst\\(\\s*" + typed_prefix + R"(\s*\)_'((?:[^'\\]|\\.)*)')"),
      "'$1'");

  // ClickHouse typed numeric literals can be rendered with suffixes (15_UInt16).
  // Also handle trailing-decimal forms such as 0._Float64.
  input = std::regex_replace(
      input,
      std::regex(R"(([+-]?[0-9]+(?:\.[0-9]*)?)_[A-Za-z][A-Za-z0-9]*(?:\([^)]*\))?(?=[^A-Za-z0-9_]|$))"),
      "$1");

  // ClickHouse may suffix string literals with explicit types ('x'_String).
  input = std::regex_replace(
      input,
      std::regex(R"('((?:[^'\\]|\\.)*)'_[A-Za-z][A-Za-z0-9]*(?:\([^)]*\))?(?=[^A-Za-z0-9_]|$))"),
      "'$1'");
  input = normalize_quoted_literal_with_trailing_parens(input);

  // ClickHouse typed constants may be wrapped in _CAST(<const>, '<Type>').
  input = std::regex_replace(
      input,
      std::regex(R"(_CAST\(\s*([+-]?[0-9]+(?:\.[0-9]*)?)(?:\s+[A-Za-z][A-Za-z0-9]*(?:\([^)]*\))?)?\s*,\s*'[^']+'(?:_[A-Za-z][A-Za-z0-9]*(?:\([^)]*\))?)?\s*\))", std::regex::icase),
      "$1");
  input = std::regex_replace(
      input,
      std::regex(R"(_CAST\(\s*([+-]?[0-9]+(?:\.[0-9]*)?)\s*\)\s*>\s*'[^']+')", std::regex::icase),
      "$1");
  input = std::regex_replace(
      input,
      std::regex(R"(_CAST\(\s*([+-]?[0-9]+(?:\.[0-9]*)?)\s*\))", std::regex::icase),
      "$1");
  input = std::regex_replace(
      input,
      std::regex(R"(([+-]?[0-9]+(?:\.[0-9]*)?)\s*>\s*'(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|Decimal(?:32|64|128|256)?(?:\([^)]*\))?|DateTime64(?:\([^)]*\))?|DateTime|Date32|Date|String|FixedString\(\d+\)|UUID|Bool|IPv[46]|Enum(?:8|16)\([^)]*\)|Nullable\([^)]*\)|LowCardinality\([^)]*\))')", std::regex::icase),
      "$1");

  // ClickHouse AST tuple literals can encode type-prefixed constants (UInt64_3, String_'AIR').
  // Match known type families to avoid rewriting regular identifiers such as column_1.
  input = std::regex_replace(input, std::regex("\\b" + typed_prefix + R"(_([+-]?[0-9]+(?:\.[0-9]*)?)\b)"), "$1");
  input = std::regex_replace(
      input,
      std::regex("\\b" + typed_prefix + R"(_'((?:[^'\\]|\\.)*)')"),
      "'$1'");

  input = normalize_quoted_literal_with_trailing_parens(input);

  return input;
}

}
