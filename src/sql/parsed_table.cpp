#include "include/dbprove/sql/parsed_table.h"

#include <dbprove/common/string.h>

#include <cctype>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sql {
namespace {

std::vector<std::string> splitTopLevelWhitespace(std::string_view text) {
  std::vector<std::string> tokens;
  size_t start = std::string_view::npos;
  int paren_depth = 0;

  for (size_t i = 0; i < text.size(); ++i) {
    const auto c = text[i];
    if (c == '(') {
      ++paren_depth;
    } else if (c == ')') {
      --paren_depth;
    }

    if (std::isspace(static_cast<unsigned char>(c)) != 0 && paren_depth == 0) {
      if (start != std::string_view::npos) {
        tokens.emplace_back(text.substr(start, i - start));
        start = std::string_view::npos;
      }
      continue;
    }

    if (start == std::string_view::npos) {
      start = i;
    }
  }

  if (start != std::string_view::npos) {
    tokens.emplace_back(text.substr(start));
  }
  return tokens;
}

SqlTypeMeta parseType(std::string_view type_spec) {
  const auto open_paren = type_spec.find('(');
  if (open_paren == std::string_view::npos) {
    return SqlTypeMeta{to_sql_type_kind(trim_string(type_spec)), SqlTypeModifier()};
  }

  const auto close_paren = type_spec.rfind(')');
  if (close_paren == std::string_view::npos || close_paren < open_paren) {
    throw std::runtime_error("Malformed type modifier in DDL: " + std::string(type_spec));
  }

  const auto kind = to_sql_type_kind(trim_string(type_spec.substr(0, open_paren)));
  const auto modifier_text = type_spec.substr(open_paren + 1, close_paren - open_paren - 1);
  const auto modifier_parts = split_top_level_by_delimiter(modifier_text, ',');

  if (kind == SqlTypeKind::STRING) {
    if (modifier_parts.size() != 1) {
      throw std::runtime_error("Expected a single string length modifier in DDL: " + std::string(type_spec));
    }
    return SqlTypeMeta{
        kind,
        SqlTypeModifier(static_cast<size_t>(std::stoull(trim_string(modifier_parts[0]))))};
  }

  if (kind == SqlTypeKind::DECIMAL) {
    if (modifier_parts.size() == 1) {
      return SqlTypeMeta{
          kind,
          SqlTypeModifier(static_cast<uint8_t>(std::stoul(trim_string(modifier_parts[0]))), 0)};
    }
    if (modifier_parts.size() == 2) {
      return SqlTypeMeta{
          kind,
          SqlTypeModifier(
              static_cast<uint8_t>(std::stoul(trim_string(modifier_parts[0]))),
              static_cast<uint8_t>(std::stoul(trim_string(modifier_parts[1]))))};
    }
    throw std::runtime_error("Expected decimal precision or precision/scale in DDL: " + std::string(type_spec));
  }

  return SqlTypeMeta{kind, SqlTypeModifier()};
}

ColumnDdl parseColumn(std::string_view definition) {
  const auto tokens = splitTopLevelWhitespace(definition);
  if (tokens.size() != 2 && tokens.size() != 3 && tokens.size() != 4) {
    throw std::runtime_error("Unable to parse column definition: " + std::string(definition));
  }

  ColumnDdl column;
  column.name = tokens[0];
  column.type = parseType(tokens[1]);
  column.is_null = true;

  if (tokens.size() == 3) {
    if (to_upper(tokens[2]) != "NULL") {
      throw std::runtime_error("Expected only NULL or NOT NULL constraint in column definition: " + std::string(definition));
    }
    column.is_null = true;
  } else if (tokens.size() == 4) {
    if (to_upper(tokens[2]) != "NOT" || to_upper(tokens[3]) != "NULL") {
      throw std::runtime_error("Expected only NULL or NOT NULL constraint in column definition: " + std::string(definition));
    }
    column.is_null = false;
  }
  return column;
}

std::string_view columnBlock(std::string_view ddl) {
  const auto open_paren = ddl.find('(');
  const auto close_paren = ddl.rfind(')');
  if (open_paren == std::string_view::npos || close_paren == std::string_view::npos || close_paren <= open_paren) {
    throw std::runtime_error("Expected CREATE TABLE DDL with a column list");
  }
  return ddl.substr(open_paren + 1, close_paren - open_paren - 1);
}

std::string extractCreateTableName(std::string_view ddl) {
  const auto trimmed = trim_string(ddl);
  std::smatch match;
  const std::regex create_table_regex(R"(^CREATE\s+TABLE\s+([^\s(]+))", std::regex::icase);
  if (!std::regex_search(trimmed, match, create_table_regex) || match.size() < 2) {
    throw std::runtime_error("Failed to extract table name from DDL: " + std::string(ddl));
  }
  return match[1].str();
}

}  // namespace

ParsedTable::ParsedTable(const std::string_view ddl) {
  table_name_ = extractCreateTableName(ddl);

  for (const auto& definition : split_top_level_by_delimiter(columnBlock(ddl), ',')) {
    if (definition.empty()) {
      continue;
    }
    columns_.push_back(parseColumn(definition));
  }

  if (columns_.empty()) {
    throw std::runtime_error("ParsedTable found no column definitions in DDL");
  }
}

}  // namespace sql
