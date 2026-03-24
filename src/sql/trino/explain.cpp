#include "connection.h"

#include <dbprove/common/string.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

#include "distribution.h"
#include "explain/plan.h"
#include "group_by.h"
#include "join.h"
#include "limit.h"
#include "projection.h"
#include "scan.h"
#include "select.h"
#include "selection.h"
#include "sequence.h"
#include "sort.h"
#include "union.h"

#include <cmath>
#include <cctype>
#include <map>
#include <optional>
#include <ranges>
#include <set>

namespace sql::trino {
using json = nlohmann::json;
using namespace sql::explain;

namespace {
std::vector<Column> parseProjectionColumns(const json& node_json);
bool projectionMatchesOutputs(const std::vector<Column>& projection_columns, const std::vector<std::string>& outputs);
bool isNumericTypedLiteral(std::string_view type_name);
bool isBooleanTypedLiteral(std::string_view type_name);
bool isPatternTypedLiteral(std::string_view type_name);
std::string normalizeOutputSymbol(std::string_view symbol);
std::map<std::string, std::string> childOutputAliases(const json& node_json);

struct ParsedAggregateAssignment {
  std::string alias;
  std::string expression;
  std::string mask;
};

std::vector<ParsedAggregateAssignment> parseAggregateAssignments(const json& node_json);

struct TrinoExpressionParser {
  struct ScanState {
    int paren_depth = 0;
    int bracket_depth = 0;
    bool in_string = false;
  };

  static void advance(ScanState& state, const std::string_view text, const size_t i) {
    const auto c = text[i];
    if (state.in_string) {
      if (c == '\'' && (i + 1 >= text.size() || text[i + 1] != '\'')) {
        state.in_string = false;
      }
      return;
    }

    switch (c) {
      case '\'':
        state.in_string = true;
        return;
      case '(':
        ++state.paren_depth;
        return;
      case ')':
        --state.paren_depth;
        return;
      case '[':
        ++state.bracket_depth;
        return;
      case ']':
        --state.bracket_depth;
        return;
      default:
        return;
    }
  }

  static bool isTopLevel(const ScanState& state) {
    return !state.in_string && state.paren_depth == 0 && state.bracket_depth == 0;
  }

  static std::optional<size_t> findTopLevelToken(const std::string_view text, const std::string_view token) {
    if (token.empty() || text.size() < token.size()) {
      return std::nullopt;
    }

    ScanState state;
    for (size_t i = 0; i + token.size() <= text.size(); ++i) {
      if (isTopLevel(state) && text.substr(i, token.size()) == token) {
        return i;
      }
      advance(state, text, i);
      if (state.in_string && i + 1 < text.size() && text[i] == '\'' && text[i + 1] == '\'') {
        ++i;
      }
    }
    return std::nullopt;
  }

  static std::vector<std::string> splitTopLevel(const std::string_view text, const char delimiter) {
    std::vector<std::string> out;
    ScanState state;
    size_t start = 0;

    for (size_t i = 0; i < text.size(); ++i) {
      if (isTopLevel(state) && text[i] == delimiter) {
        out.push_back(trim_string(text.substr(start, i - start)));
        start = i + 1;
      }
      advance(state, text, i);
      if (state.in_string && i + 1 < text.size() && text[i] == '\'' && text[i + 1] == '\'') {
        ++i;
      }
    }

    out.push_back(trim_string(text.substr(start)));
    return out;
  }

  static std::optional<size_t> findMatchingParen(const std::string_view text, const size_t open_pos) {
    if (open_pos >= text.size() || text[open_pos] != '(') {
      return std::nullopt;
    }

    ScanState state;
    for (size_t i = open_pos; i < text.size(); ++i) {
      advance(state, text, i);
      if (!state.in_string && state.paren_depth == 0) {
        return i;
      }
      if (state.in_string && i + 1 < text.size() && text[i] == '\'' && text[i + 1] == '\'') {
        ++i;
      }
    }

    return std::nullopt;
  }

  static std::optional<std::pair<std::string, std::string>> parseAssignment(const std::string_view detail) {
    const auto assign_pos = findTopLevelToken(detail, ":=");
    if (!assign_pos.has_value()) {
      return std::nullopt;
    }
    return std::make_pair(
      trim_string(detail.substr(0, *assign_pos)),
      trim_string(detail.substr(*assign_pos + 2)));
  }

  static bool isIdentifierStart(const char c) {
    return std::isalpha(static_cast<unsigned char>(c)) != 0 || c == '_';
  }

  static bool isIdentifierChar(const char c) {
    return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' || c == '.';
  }

  static size_t readQuotedString(const std::string_view text, const size_t start) {
    size_t i = start + 1;
    while (i < text.size()) {
      if (text[i] == '\'') {
        ++i;
        if (i < text.size() && text[i] == '\'') {
          ++i;
          continue;
        }
        break;
      }
      ++i;
    }
    return i;
  }

  static std::optional<char> previousNonWhitespaceChar(const std::string_view text, size_t index) {
    while (index > 0) {
      --index;
      if (std::isspace(static_cast<unsigned char>(text[index])) == 0) {
        return text[index];
      }
    }
    return std::nullopt;
  }

  static bool isFunctionCallContext(const std::string_view text, const size_t token_start, const size_t next_nonspace) {
    if (next_nonspace >= text.size() || text[next_nonspace] != '(') {
      return false;
    }

    const auto previous = previousNonWhitespaceChar(text, token_start);
    if (!previous.has_value()) {
      return true;
    }

    return !isIdentifierChar(*previous) && *previous != ')' && *previous != '\'';
  }

  static std::string replaceIdentifiers(
    const std::string_view expression,
    const std::map<std::string, std::string>& assignments) {
    if (expression.empty() || assignments.empty()) {
      return std::string(expression);
    }

    std::string resolved_expression;
    resolved_expression.reserve(expression.size());

    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        resolved_expression += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }

      if (!isIdentifierChar(expression[i])) {
        resolved_expression += expression[i++];
        continue;
      }

      size_t end = i + 1;
      while (end < expression.size() && isIdentifierChar(expression[end])) {
        ++end;
      }

      const auto token = std::string(expression.substr(i, end - i));
      const auto found = assignments.find(token);
      if (found == assignments.end() || found->second.empty() || found->second == token) {
        resolved_expression += token;
      } else {
        const auto replacement = found->second.find_first_of(" ()[],") == std::string::npos
                                   ? found->second
                                   : "(" + found->second + ")";
        resolved_expression += replacement;
      }
      i = end;
    }

    return resolved_expression;
  }

  static std::optional<std::pair<std::string, std::string>> splitAggregateMask(const std::string_view expression) {
    const auto open_pos = expression.find('(');
    if (open_pos == std::string::npos) {
      return std::nullopt;
    }

    const auto close_pos = findMatchingParen(expression, open_pos);
    if (!close_pos.has_value()) {
      return std::nullopt;
    }

    const auto base_expression = trim_string(expression.substr(0, *close_pos + 1));
    auto remainder = trim_string(expression.substr(*close_pos + 1));
    if (remainder.empty()) {
      return std::nullopt;
    }

    remainder = strip_enclosing(remainder, '(', ')');
    const auto mask_pos = findTopLevelToken(remainder, "=");
    if (!mask_pos.has_value()) {
      return std::nullopt;
    }

    const auto lhs = to_lower(trim_string(remainder.substr(0, *mask_pos)));
    const auto rhs = trim_string(remainder.substr(*mask_pos + 1));
    if (lhs != "mask" || rhs.empty()) {
      return std::nullopt;
    }

    return std::make_pair(base_expression, rhs);
  }

  static std::string normalizeTypedLiterals(const std::string_view expression) {
    std::string normalized;
    normalized.reserve(expression.size());

    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        normalized += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }

      if (!isIdentifierStart(expression[i])) {
        normalized += expression[i++];
        continue;
      }

      const auto type_start = i;
      size_t type_end = i + 1;
      while (type_end < expression.size() &&
             (std::isalnum(static_cast<unsigned char>(expression[type_end])) != 0 || expression[type_end] == '_')) {
        ++type_end;
      }
      if (type_end < expression.size() && expression[type_end] == '(') {
        const auto close = findMatchingParen(expression, type_end);
        if (close.has_value()) {
          type_end = *close + 1;
        }
      }

      size_t literal_start = type_end;
      while (literal_start < expression.size() &&
             std::isspace(static_cast<unsigned char>(expression[literal_start])) != 0) {
        ++literal_start;
      }
      if (literal_start >= expression.size() || expression[literal_start] != '\'') {
        normalized += std::string(expression.substr(type_start, type_end - type_start));
        i = type_end;
        continue;
      }

      const auto literal_end = readQuotedString(expression, literal_start);
      const auto literal_type = std::string(expression.substr(type_start, type_end - type_start));
      const auto literal_value = std::string(expression.substr(literal_start, literal_end - literal_start));
      if (isNumericTypedLiteral(literal_type)) {
        normalized += literal_value.substr(1, literal_value.size() - 2);
      } else if (isBooleanTypedLiteral(literal_type)) {
        normalized += to_lower(literal_value.substr(1, literal_value.size() - 2));
      } else if (isPatternTypedLiteral(literal_type)) {
        auto pattern = literal_value.substr(1, literal_value.size() - 2);
        if (pattern.size() >= 2 && pattern.front() == '[' && pattern.back() == ']') {
          pattern = pattern.substr(1, pattern.size() - 2);
        }
        normalized += "'" + pattern + "'";
      } else {
        normalized += literal_value;
      }
      i = literal_end;
    }

    return normalized;
  }

  static std::string stripCatalogPrefixes(const std::string_view expression) {
    std::string out;
    out.reserve(expression.size());

    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        out += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }

      if (!isIdentifierStart(expression[i])) {
        out += expression[i++];
        continue;
      }

      size_t end = i + 1;
      while (end < expression.size() && isIdentifierChar(expression[end])) {
        ++end;
      }
      if (end < expression.size() && expression[end] == ':' &&
          (end + 1 >= expression.size() || expression[end + 1] != ':')) {
        i = end + 1;
        continue;
      }

      out += std::string(expression.substr(i, end - i));
      i = end;
    }

    return out;
  }

  static std::string stripQuotedLiteralTypePrefix(
    const std::string_view expression,
    const std::string_view type_name) {
    std::string out;
    out.reserve(expression.size());

    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        out += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }

      if (!isIdentifierStart(expression[i])) {
        out += expression[i++];
        continue;
      }

      size_t end = i + 1;
      while (end < expression.size() &&
             (std::isalnum(static_cast<unsigned char>(expression[end])) != 0 || expression[end] == '_')) {
        ++end;
      }

      const auto token = expression.substr(i, end - i);
      size_t next = end;
      while (next < expression.size() && std::isspace(static_cast<unsigned char>(expression[next])) != 0) {
        ++next;
      }

      if (token == type_name && next < expression.size() && expression[next] == '\'') {
        i = end;
        continue;
      }

      out += std::string(expression.substr(i, end - i));
      i = end;
    }

    return out;
  }

  static std::string replaceOutsideStrings(
    const std::string_view expression,
    const std::string_view needle,
    const std::string_view replacement) {
    if (needle.empty()) {
      return std::string(expression);
    }

    std::string out;
    out.reserve(expression.size());
    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        out += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }
      if (i + needle.size() <= expression.size() && expression.substr(i, needle.size()) == needle) {
        out += replacement;
        i += needle.size();
        continue;
      }
      out += expression[i++];
    }
    return out;
  }

  static std::string renameFunctions(const std::string_view expression) {
    static const std::map<std::string, std::string> replacements = {
      {"add", "plus"},
      {"subtract", "minus"},
      {"like", "funcLike"},
      {"and", "funcAnd"},
      {"or", "funcOr"},
    };

    std::string out;
    out.reserve(expression.size());
    for (size_t i = 0; i < expression.size();) {
      if (expression[i] == '\'') {
        const auto end = readQuotedString(expression, i);
        out += std::string(expression.substr(i, end - i));
        i = end;
        continue;
      }
      if (!isIdentifierStart(expression[i])) {
        out += expression[i++];
        continue;
      }

      size_t end = i + 1;
      while (end < expression.size() &&
             (std::isalnum(static_cast<unsigned char>(expression[end])) != 0 || expression[end] == '_')) {
        ++end;
      }
      size_t next = end;
      while (next < expression.size() && std::isspace(static_cast<unsigned char>(expression[next])) != 0) {
        ++next;
      }

      const auto token = to_lower(expression.substr(i, end - i));
      const auto requires_call_context = token == "and" || token == "or";
      const auto should_replace =
        replacements.contains(token) &&
        next < expression.size() &&
        expression[next] == '(' &&
        (!requires_call_context || isFunctionCallContext(expression, i, next));
      if (should_replace) {
        out += replacements.at(token);
      } else {
        out += std::string(expression.substr(i, end - i));
      }
      i = end;
    }
    return out;
  }

  static std::string stripOrderByDecorators(std::string expression) {
    auto consume_suffix = [&](const std::string_view suffix) {
      const auto upper = to_upper(expression);
      if (upper.size() < suffix.size() || upper.substr(upper.size() - suffix.size()) != suffix) {
        return false;
      }
      expression = trim_string(expression.substr(0, expression.size() - suffix.size()));
      return true;
    };

    consume_suffix(" NULLS FIRST");
    consume_suffix(" NULLS LAST");
    if (consume_suffix(" DESC")) {
      return expression;
    }
    consume_suffix(" ASC");
    return expression;
  }

  static std::string stripTrailingTypeCast(const std::string_view expression) {
    ScanState state;
    std::optional<size_t> cast_pos;
    for (size_t i = 0; i + 1 < expression.size(); ++i) {
      if (isTopLevel(state) && expression[i] == ':' && expression[i + 1] == ':') {
        cast_pos = i;
      }
      advance(state, expression, i);
      if (state.in_string && i + 1 < expression.size() && expression[i] == '\'' && expression[i + 1] == '\'') {
        ++i;
      }
    }
    if (!cast_pos.has_value()) {
      return trim_string(expression);
    }
    return trim_string(expression.substr(0, *cast_pos));
  }
};

bool isNumericTypedLiteral(const std::string_view type_name) {
  const auto upper = to_upper(type_name);
  return upper == "BIGINT" ||
         upper == "INTEGER" ||
         upper == "INT" ||
         upper == "SMALLINT" ||
         upper == "TINYINT" ||
         upper == "DOUBLE" ||
         upper == "REAL" ||
         upper.starts_with("DECIMAL");
}

bool isBooleanTypedLiteral(const std::string_view type_name) {
  return to_upper(type_name) == "BOOLEAN";
}

bool isPatternTypedLiteral(const std::string_view type_name) {
  return to_upper(type_name) == "LIKEPATTERN";
}

bool isReservedOutputSymbol(const std::string_view symbol) {
  static const std::set<std::string, std::less<>> reserved = {
    "false",
    "null",
    "true",
  };

  return reserved.contains(to_lower(trim_string(symbol)));
}

std::string normalizeOutputSymbol(const std::string_view symbol) {
  auto normalized = trim_string(symbol);
  if (normalized.empty()) {
    return normalized;
  }

  if (isReservedOutputSymbol(normalized)) {
    normalized += "_value";
  }

  for (auto& c : normalized) {
    if (!TrinoExpressionParser::isIdentifierChar(c)) {
      c = '_';
    }
  }

  if (normalized.empty() || !TrinoExpressionParser::isIdentifierStart(normalized.front())) {
    normalized = "column_" + normalized;
  }

  return normalized;
}

std::string cleanTrinoExpression(std::string expression) {
  expression = TrinoExpressionParser::normalizeTypedLiterals(expression);
  expression = TrinoExpressionParser::replaceOutsideStrings(expression, "LikePattern ", "");
  expression = TrinoExpressionParser::stripQuotedLiteralTypePrefix(expression, "LikePattern");
  expression = TrinoExpressionParser::stripCatalogPrefixes(expression);
  expression = TrinoExpressionParser::replaceOutsideStrings(expression, "$operator$", "");
  expression = TrinoExpressionParser::replaceOutsideStrings(expression, "$", "");
  expression = TrinoExpressionParser::renameFunctions(expression);
  return cleanExpression(trim_string(expression));
}

double parseEstimateValue(const json& value) {
  if (value.is_number()) {
    return value.get<double>();
  }
  if (value.is_string()) {
    const auto s = value.get<std::string>();
    if (s == "NaN") {
      return NAN;
    }
    try {
      return std::stod(s);
    } catch (...) {
      return NAN;
    }
  }
  return NAN;
}

void applyNodeMetadata(Node& node, const json& node_json) {
  if (node_json.contains("outputs") && node_json["outputs"].is_array()) {
    for (const auto& output : node_json["outputs"]) {
      node.columns_output.push_back(normalizeOutputSymbol(output.value("name", "")));
    }
  }

  if (node_json.contains("estimates") && node_json["estimates"].is_array() && !node_json["estimates"].empty()) {
    const auto& estimate = node_json["estimates"].front();
    if (estimate.contains("outputRowCount")) {
      node.rows_estimated = parseEstimateValue(estimate["outputRowCount"]);
    }
  }
}

std::string descriptorString(const json& node_json, const std::string& key) {
  if (!node_json.contains("descriptor") || !node_json["descriptor"].is_object()) {
    return "";
  }
  const auto& descriptor = node_json["descriptor"];
  if (!descriptor.contains(key) || !descriptor[key].is_string()) {
    return "";
  }
  return descriptor[key].get<std::string>();
}

std::vector<std::string> outputNames(const json& node_json) {
  std::vector<std::string> result;
  if (!node_json.contains("outputs") || !node_json["outputs"].is_array()) {
    return result;
  }
  for (const auto& output : node_json["outputs"]) {
    result.push_back(normalizeOutputSymbol(output.value("name", "")));
  }
  return result;
}

std::map<std::string, std::string> childOutputAliases(const json& node_json) {
  std::map<std::string, std::string> aliases;
  if (!node_json.contains("children") || !node_json["children"].is_array()) {
    return aliases;
  }

  for (const auto& child : node_json["children"]) {
    if (!child.contains("outputs") || !child["outputs"].is_array()) {
      continue;
    }
    for (const auto& output : child["outputs"]) {
      const auto raw = output.value("name", "");
      const auto normalized = normalizeOutputSymbol(raw);
      if (raw != normalized) {
        aliases[raw] = normalized;
      }
    }
  }
  return aliases;
}

std::map<std::string, std::string> parseAssignments(const json& node_json) {
  std::map<std::string, std::string> assignments;
  if (!node_json.contains("details") || !node_json["details"].is_array()) {
    return assignments;
  }

  const auto aliases = childOutputAliases(node_json);
  for (const auto& detail_json : node_json["details"]) {
    const auto detail = detail_json.get<std::string>();
    const auto parsed = TrinoExpressionParser::parseAssignment(detail);
    if (!parsed.has_value()) {
      continue;
    }
    const auto& [lhs, raw_rhs] = *parsed;
    auto rhs = cleanTrinoExpression(raw_rhs);
    rhs = TrinoExpressionParser::replaceIdentifiers(rhs, aliases);
    const auto normalized_lhs = normalizeOutputSymbol(lhs);
    assignments[normalized_lhs] = rhs;
    if (normalized_lhs != lhs) {
      assignments[lhs] = rhs;
    }
  }
  return assignments;
}

std::string resolveAssignedSymbols(std::string expression, const std::map<std::string, std::string>& assignments) {
  if (expression.empty() || assignments.empty()) {
    return expression;
  }

  return TrinoExpressionParser::replaceIdentifiers(expression, assignments);
}

std::string resolveNodeExpression(const json& node_json, std::string expression) {
  expression = cleanTrinoExpression(std::move(expression));
  expression = resolveAssignedSymbols(std::move(expression), parseAssignments(node_json));
  return TrinoExpressionParser::replaceIdentifiers(expression, childOutputAliases(node_json));
}

std::string resolveJoinExpression(const json& node_json, std::string expression) {
  expression = cleanTrinoExpression(std::move(expression));
  return TrinoExpressionParser::replaceIdentifiers(expression, childOutputAliases(node_json));
}

std::vector<Column> parseProjectionColumns(const json& node_json) {
  const auto outputs = outputNames(node_json);
  auto final_outputs = outputs;
  const auto assignments = parseAssignments(node_json);

  const auto descriptor_outputs = descriptorString(node_json, "columnNames");
  if (!descriptor_outputs.empty()) {
    const auto parsed = TrinoExpressionParser::splitTopLevel(strip_enclosing(descriptor_outputs, '[', ']'), ',');
    if (parsed.size() == outputs.size()) {
      final_outputs.clear();
      final_outputs.reserve(parsed.size());
      for (const auto& output : parsed) {
        final_outputs.push_back(normalizeOutputSymbol(output));
      }
    }
  }

  std::vector<Column> result;
  for (size_t i = 0; i < outputs.size(); ++i) {
    const auto& output_name = outputs[i];
    const auto& final_name = final_outputs[i];
    const auto found = assignments.find(final_name);
    if (found != assignments.end() && found->second != output_name) {
      result.emplace_back(found->second, final_name);
    } else if (final_name != output_name) {
      result.emplace_back(output_name, final_name);
    } else {
      result.emplace_back(output_name);
    }
  }
  return result;
}

std::unique_ptr<Projection> makeProjection(std::vector<Column> columns) {
  auto projection = std::make_unique<Projection>(columns);
  projection->include_input_columns = false;
  return projection;
}

std::vector<Column> parseOrderBy(const std::string& order_by_raw) {
  std::vector<Column> result;
  for (auto part : TrinoExpressionParser::splitTopLevel(strip_enclosing(order_by_raw, '[', ']'), ',')) {
    auto sorting = Column::Sorting::ASC;
    if (part.contains(" DESC")) {
      sorting = Column::Sorting::DESC;
    }
    part = TrinoExpressionParser::stripOrderByDecorators(part);
    result.emplace_back(cleanTrinoExpression(part), sorting);
  }
  return result;
}

std::vector<Column> parseGroupKeys(const json& node_json) {
  if (!node_json.contains("descriptor") || !node_json["descriptor"].is_object()) {
    return {};
  }
  const auto& descriptor = node_json["descriptor"];
  if (!descriptor.contains("keys")) {
    return {};
  }

  const auto raw_keys = trim_string(descriptor["keys"].get<std::string>());
  if (raw_keys.empty()) {
    return {};
  }

  std::vector<Column> keys;
  for (const auto& key : TrinoExpressionParser::splitTopLevel(strip_enclosing(raw_keys, '[', ']'), ',')) {
    keys.emplace_back(resolveNodeExpression(node_json, key));
  }
  return keys;
}

std::vector<Column> parseAggregates(const json& node_json, const std::vector<Column>& group_keys) {
  std::vector<Column> aggregates;
  for (const auto& aggregate : parseAggregateAssignments(node_json)) {
    aggregates.emplace_back(aggregate.expression, aggregate.alias);
  }

  if (!aggregates.empty()) {
    return aggregates;
  }

  std::set<std::string> group_key_names;
  for (const auto& key : group_keys) {
    group_key_names.insert(key.name);
  }

  for (const auto& output_name : outputNames(node_json)) {
    if (!group_key_names.contains(output_name)) {
      aggregates.emplace_back(output_name);
    }
  }
  return aggregates;
}

std::vector<ParsedAggregateAssignment> parseAggregateAssignments(const json& node_json) {
  std::vector<ParsedAggregateAssignment> aggregates;
  if (!node_json.contains("details") || !node_json["details"].is_array()) {
    return aggregates;
  }

  for (const auto& detail_json : node_json["details"]) {
    const auto parsed = TrinoExpressionParser::parseAssignment(detail_json.get<std::string>());
    if (!parsed.has_value()) {
      continue;
    }

    const auto& [alias, raw_expression] = *parsed;
    ParsedAggregateAssignment aggregate{.alias = normalizeOutputSymbol(alias)};
    if (const auto masked = TrinoExpressionParser::splitAggregateMask(raw_expression); masked.has_value()) {
      aggregate.expression = cleanTrinoExpression(masked->first);
      aggregate.mask = cleanTrinoExpression(masked->second);
    } else {
      aggregate.expression = cleanTrinoExpression(raw_expression);
    }
    const auto aliases = childOutputAliases(node_json);
    aggregate.expression = TrinoExpressionParser::replaceIdentifiers(aggregate.expression, aliases);
    aggregate.mask = TrinoExpressionParser::replaceIdentifiers(aggregate.mask, aliases);
    aggregates.push_back(std::move(aggregate));
  }

  return aggregates;
}

std::optional<std::string> sharedAggregateMask(const std::vector<ParsedAggregateAssignment>& aggregates) {
  if (aggregates.empty()) {
    return std::nullopt;
  }

  const auto& first_mask = aggregates.front().mask;
  if (first_mask.empty()) {
    return std::nullopt;
  }

  for (const auto& aggregate : aggregates) {
    if (aggregate.mask != first_mask) {
      return std::nullopt;
    }
  }

  return first_mask;
}

std::vector<Column> parseDistributionArguments(const json& node_json) {
  if (!node_json.contains("descriptor") || !node_json["descriptor"].is_object()) {
    return {};
  }
  const auto& descriptor = node_json["descriptor"];
  if (!descriptor.contains("arguments")) {
    return {};
  }

  std::vector<Column> columns;
  for (auto arg : TrinoExpressionParser::splitTopLevel(strip_enclosing(descriptor["arguments"].get<std::string>(), '[', ']'), ',')) {
    arg = TrinoExpressionParser::stripTrailingTypeCast(arg);
    if (!trim_string(arg).empty()) {
      columns.emplace_back(resolveNodeExpression(node_json, arg));
    }
  }
  return columns;
}

std::vector<Column> parseValuesColumns(const json& node_json) {
  std::vector<Column> columns;
  if (!node_json.contains("details") || !node_json["details"].is_array() || node_json["details"].empty()) {
    return columns;
  }

  const auto outputs = outputNames(node_json);
  auto raw_values = trim_string(node_json["details"].front().get<std::string>());
  raw_values = strip_enclosing(raw_values, '(', ')');
  const auto values = TrinoExpressionParser::splitTopLevel(raw_values, ',');
  if (values.size() != outputs.size()) {
    return columns;
  }

  columns.reserve(outputs.size());
  for (size_t i = 0; i < outputs.size(); ++i) {
    columns.emplace_back(cleanTrinoExpression(values[i]), outputs[i]);
  }
  return columns;
}

std::string parseTableName(const std::string& raw_table) {
  std::vector<std::string> parts;
  size_t start = 0;
  for (size_t i = 0; i <= raw_table.size(); ++i) {
    if (i == raw_table.size() || raw_table[i] == ':') {
      parts.push_back(trim_string(std::string_view(raw_table).substr(start, i - start)));
      start = i + 1;
    }
  }
  if (parts.size() == 3) {
    if (parts[0] == "tpch" && parts[1] == "sf1") {
      return "tpch." + parts[2];
    }
    return parts[1] + "." + parts[2];
  }
  return raw_table;
}

bool projectionMatchesOutputs(const std::vector<Column>& projection_columns, const std::vector<std::string>& outputs) {
  if (projection_columns.size() != outputs.size()) {
    return false;
  }
  for (size_t i = 0; i < outputs.size(); ++i) {
    const auto& column = projection_columns[i];
    if (column.name != outputs[i] || column.hasAlias()) {
      return false;
    }
  }
  return true;
}

std::unique_ptr<Node> attachChild(std::unique_ptr<Node> parent, std::unique_ptr<Node> child) {
  if (child) {
    parent->addChild(std::move(child));
  }
  return parent;
}

std::vector<std::string> fragmentIdsFromSource(const json& node_json) {
  const auto raw = descriptorString(node_json, "sourceFragmentIds");
  if (raw.empty()) {
    return {};
  }
  return TrinoExpressionParser::splitTopLevel(strip_enclosing(raw, '[', ']'), ',');
}

std::unique_ptr<Node> buildExplainNode(const json& node_json, const std::map<std::string, json>& fragments);

std::unique_ptr<Node> buildSourceFragments(const json& node_json, const std::map<std::string, json>& fragments) {
  const auto fragment_ids = fragmentIdsFromSource(node_json);
  if (fragment_ids.empty()) {
    return nullptr;
  }

  if (fragment_ids.size() == 1) {
    const auto found = fragments.find(trim_string(fragment_ids.front()));
    if (found == fragments.end()) {
      throw InvalidPlanException("Trino EXPLAIN referenced missing fragment id " + trim_string(fragment_ids.front()));
    }
    return buildExplainNode(found->second, fragments);
  }

  auto union_node = std::make_unique<Union>(Union::Type::ALL);
  for (const auto& fragment_id : fragment_ids) {
    const auto found = fragments.find(trim_string(fragment_id));
    if (found == fragments.end()) {
      throw InvalidPlanException("Trino EXPLAIN referenced missing fragment id " + trim_string(fragment_id));
    }
    union_node->addChild(buildExplainNode(found->second, fragments));
  }
  return union_node;
}

std::unique_ptr<Node> buildExplainNode(const json& node_json, const std::map<std::string, json>& fragments) {
  const auto name = node_json.value("name", "");

  std::vector<std::unique_ptr<Node>> built_children;
  if (node_json.contains("children") && node_json["children"].is_array()) {
    for (const auto& child_json : node_json["children"]) {
      auto child = buildExplainNode(child_json, fragments);
      if (child) {
        built_children.push_back(std::move(child));
      }
    }
  }

  auto take_first_child = [&]() -> std::unique_ptr<Node> {
    if (built_children.empty()) {
      return nullptr;
    }
    auto child = std::move(built_children.front());
    built_children.erase(built_children.begin());
    return child;
  };

  auto attach_children = [&](std::unique_ptr<Node> node) -> std::unique_ptr<Node> {
    for (auto& child : built_children) {
      node->addChild(std::move(child));
    }
    applyNodeMetadata(*node, node_json);
    return node;
  };

  if (name == "RemoteSource" || name == "RemoteMerge") {
    return buildSourceFragments(node_json, fragments);
  }

  if (name == "AssignUniqueId" || name == "PartialSort") {
    return take_first_child();
  }

  if (name == "Output") {
    auto child = take_first_child();
    const auto projection_columns = parseProjectionColumns(node_json);
    const auto outputs = outputNames(node_json);
    if (!child || projection_columns.empty() || projectionMatchesOutputs(projection_columns, outputs)) {
      return child;
    }
    auto projection = makeProjection(projection_columns);
    applyNodeMetadata(*projection, node_json);
    return attachChild(std::move(projection), std::move(child));
  }

  if (name == "LocalMerge") {
    auto node = std::make_unique<Sort>(parseOrderBy(descriptorString(node_json, "orderBy")));
    applyNodeMetadata(*node, node_json);
    return attachChild(std::move(node), take_first_child());
  }

  if (name == "LocalExchange") {
    auto strategy = Distribute::Strategy::GATHER;
    const auto partitioning = to_upper(descriptorString(node_json, "partitioning"));
    if (partitioning == "HASH") {
      strategy = Distribute::Strategy::HASH;
    } else if (partitioning == "ROUND_ROBIN") {
      strategy = Distribute::Strategy::ROUND_ROBIN;
    }
    auto node = std::make_unique<Distribute>(strategy, parseDistributionArguments(node_json));
    applyNodeMetadata(*node, node_json);
    return attachChild(std::move(node), take_first_child());
  }

  if (name == "TopN") {
    auto child = take_first_child();
    auto sort = std::make_unique<Sort>(parseOrderBy(descriptorString(node_json, "orderBy")));
    applyNodeMetadata(*sort, node_json);
    sort->addChild(std::move(child));
    auto limit_count = static_cast<RowCount>(std::stoull(descriptorString(node_json, "count")));
    auto limit = std::make_unique<Limit>(limit_count);
    applyNodeMetadata(*limit, node_json);
    limit->addChild(std::move(sort));
    return limit;
  }

  if (name == "TopNPartial") {
    auto node = std::make_unique<Sort>(parseOrderBy(descriptorString(node_json, "orderBy")));
    applyNodeMetadata(*node, node_json);
    return attachChild(std::move(node), take_first_child());
  }

  if (name == "TableScan") {
    std::unique_ptr<Node> node = std::make_unique<Scan>(parseTableName(descriptorString(node_json, "table")));
    applyNodeMetadata(*node, node_json);

    const auto projection_columns = parseProjectionColumns(node_json);
    const auto outputs = outputNames(node_json);
    if (!projection_columns.empty() && !projectionMatchesOutputs(projection_columns, outputs)) {
      auto projection = makeProjection(projection_columns);
      applyNodeMetadata(*projection, node_json);
      projection->addChild(std::move(node));
      node = std::move(projection);
    }
    return node;
  }

  if (name == "ScanFilter" || name == "ScanProject" || name == "ScanFilterProject") {
    std::unique_ptr<Node> node = std::make_unique<Scan>(parseTableName(descriptorString(node_json, "table")));
    applyNodeMetadata(*node, node_json);

    const auto filter = trim_string(descriptorString(node_json, "filterPredicate"));
    if (!filter.empty()) {
      auto selection = std::make_unique<Selection>(resolveNodeExpression(node_json, filter));
      applyNodeMetadata(*selection, node_json);
      selection->addChild(std::move(node));
      node = std::move(selection);
    }

    auto projection_columns = parseProjectionColumns(node_json);
    const auto outputs = outputNames(node_json);
    if (!projection_columns.empty() && !projectionMatchesOutputs(projection_columns, outputs)) {
      auto projection = makeProjection(projection_columns);
      applyNodeMetadata(*projection, node_json);
      projection->addChild(std::move(node));
      node = std::move(projection);
    }
    return node;
  }

  if (name == "Project") {
    auto child = take_first_child();
    auto projection_columns = parseProjectionColumns(node_json);
    const auto outputs = outputNames(node_json);
    if (!child || projection_columns.empty() || projectionMatchesOutputs(projection_columns, outputs)) {
      return child;
    }
    auto projection = makeProjection(projection_columns);
    applyNodeMetadata(*projection, node_json);
    projection->addChild(std::move(child));
    return projection;
  }

  if (name == "FilterProject") {
    auto child = take_first_child();
    const auto filter = trim_string(descriptorString(node_json, "filterPredicate"));
    if (!filter.empty()) {
      auto selection = std::make_unique<Selection>(resolveNodeExpression(node_json, filter));
      applyNodeMetadata(*selection, node_json);
      selection->addChild(std::move(child));
      child = std::move(selection);
    }

    const auto projection_columns = parseProjectionColumns(node_json);
    const auto outputs = outputNames(node_json);
    if (!projection_columns.empty() && !projectionMatchesOutputs(projection_columns, outputs)) {
      auto projection = makeProjection(projection_columns);
      applyNodeMetadata(*projection, node_json);
      projection->addChild(std::move(child));
      return projection;
    }
    return child;
  }

  if (name == "Aggregate") {
    const auto group_keys = parseGroupKeys(node_json);
    const auto aggregate_assignments = parseAggregateAssignments(node_json);
    const auto aggregates = parseAggregates(node_json, group_keys);
    auto strategy = GroupBy::Strategy::HASH;
    const auto aggregate_type = descriptorString(node_json, "type");
    if (aggregate_type.contains("PARTIAL")) {
      strategy = GroupBy::Strategy::PARTIAL;
    } else if (aggregate_type.contains("STREAMING")) {
      strategy = GroupBy::Strategy::SORT_MERGE;
    } else if (group_keys.empty()) {
      strategy = GroupBy::Strategy::SIMPLE;
    }

    auto group_by = std::make_unique<GroupBy>(strategy, group_keys, aggregates);
    applyNodeMetadata(*group_by, node_json);
    auto child = take_first_child();
    if (const auto mask = sharedAggregateMask(aggregate_assignments); mask.has_value()) {
      auto selection = std::make_unique<Selection>(*mask);
      selection->addChild(std::move(child));
      child = std::move(selection);
    }
    group_by->addChild(std::move(child));
    return group_by;
  }

  if (name == "InnerJoin" || name == "LeftJoin" || name == "RightJoin" || name == "CrossJoin" || name == "SemiJoin") {
    auto join_type = Join::Type::INNER;
    auto join_strategy = Join::Strategy::HASH;
    auto condition = resolveJoinExpression(node_json, descriptorString(node_json, "criteria"));

    if (name == "LeftJoin") {
      join_type = Join::Type::LEFT_OUTER;
    } else if (name == "RightJoin") {
      join_type = Join::Type::RIGHT_OUTER;
    } else if (name == "CrossJoin") {
      join_type = Join::Type::CROSS;
      join_strategy = Join::Strategy::LOOP;
      condition.clear();
    } else if (name == "SemiJoin") {
      join_type = Join::Type::LEFT_SEMI_INNER;
    }

    auto join = std::make_unique<Join>(join_type, join_strategy, condition);
    return attach_children(std::move(join));
  }

  if (name == "Values") {
    auto select = std::make_unique<Select>();
    applyNodeMetadata(*select, node_json);
    const auto values_columns = parseValuesColumns(node_json);
    if (values_columns.empty()) {
      return select;
    }
    auto projection = makeProjection(values_columns);
    applyNodeMetadata(*projection, node_json);
    projection->addChild(std::move(select));
    return projection;
  }

  if (built_children.size() == 1) {
    return take_first_child();
  }

  if (!built_children.empty()) {
    auto sequence = std::make_unique<Sequence>();
    return attach_children(std::move(sequence));
  }

  auto fallback = std::make_unique<Select>();
  applyNodeMetadata(*fallback, node_json);
  return fallback;
}

std::unique_ptr<Plan> buildExplainPlan(const json& explain_json) {
  if (!explain_json.is_object() || !explain_json.contains("0")) {
    throw InvalidPlanException("Trino EXPLAIN JSON did not contain a root fragment");
  }

  std::map<std::string, json> fragments;
  for (auto it = explain_json.begin(); it != explain_json.end(); ++it) {
    fragments[it.key()] = it.value();
  }

  auto root = buildExplainNode(explain_json["0"], fragments);
  if (!root) {
    throw InvalidPlanException("Trino EXPLAIN JSON did not produce a canonical plan");
  }

  return std::make_unique<Plan>(std::move(root));
}
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string artifact_name = name.has_value() ? std::string(*name) : std::to_string(std::hash<std::string_view>{}(statement));
  if (const auto cached_json = getArtefact(artifact_name, "json")) {
    PLOGI << "Using cached Trino execution plan artifact for: " << artifact_name;
    auto plan = buildExplainPlan(json::parse(*cached_json));
    const auto* skip_actuals_env = std::getenv("DBPROVE_SKIP_ACTUALS");
    const bool skip_actuals = skip_actuals_env != nullptr &&
                              std::string_view(skip_actuals_env) == "1";
    if (!skip_actuals) {
      plan->fixActuals(*this);
    }
    return plan;
  }

  const auto explain_sql = "EXPLAIN (FORMAT JSON)\n" + std::string(statement);
  const auto explain_string = fetchScalar(explain_sql).asString();
  storeArtefact(artifact_name, "json", explain_string);
  auto plan = buildExplainPlan(json::parse(explain_string));
  const auto* skip_actuals_env = std::getenv("DBPROVE_SKIP_ACTUALS");
  const bool skip_actuals = skip_actuals_env != nullptr &&
                            std::string_view(skip_actuals_env) == "1";
  if (!skip_actuals) {
    plan->fixActuals(*this);
  }
  return plan;
}
}
