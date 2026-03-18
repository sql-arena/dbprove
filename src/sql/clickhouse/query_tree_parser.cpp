#include "query_tree_parser.h"

#include <cctype>
#include <map>
#include <regex>
#include <sstream>

#include "connection.h"
#include "sql.h"
#include <dbprove/common/string.h>

namespace sql::clickhouse {
std::string fetchClickHouseExplainQueryTree(Connection& connection,
                                            const std::string_view statement,
                                            const std::string_view artifact_name,
                                            const std::optional<std::string>& cached_query_tree) {
  if (cached_query_tree.has_value()) {
    return *cached_query_tree;
  }

  const std::string explain_query_tree_stmt = "EXPLAIN QUERY TREE\n"
                                              + std::string(statement) + "\nFORMAT TSVRaw";
  auto query_tree_result = connection.fetchAll(explain_query_tree_stmt);
  std::string query_tree;
  for (auto& row : query_tree_result->rows()) {
    query_tree += row[0].asString();
    query_tree += "\n";
  }
  connection.storeArtefact(artifact_name, "query_tree", query_tree);
  return query_tree;
}

size_t countUncorrelatedScalarSubqueriesInQueryTree(const std::string_view query_tree) {
  size_t uncorrelated_scalar_count = 0;
  const std::string query_tree_string(query_tree);
  const std::regex scalar_subquery_query_line(
      R"(CONSTANT[^\n]*\n\s+EXPRESSION\n\s+QUERY id:\s*\d+,\s*is_subquery:\s*1(?:,\s*is_correlated:\s*1)?)",
      std::regex::icase);
  std::sregex_iterator it(query_tree_string.begin(), query_tree_string.end(), scalar_subquery_query_line);
  std::sregex_iterator end;
  for (; it != end; ++it) {
    const auto match_text = it->str();
    if (std::regex_search(match_text, std::regex(R"(is_correlated:\s*1)", std::regex::icase))) {
      continue;
    }
    ++uncorrelated_scalar_count;
  }
  return uncorrelated_scalar_count;
}

std::vector<InSubqueryRelationGuess> guessInSubqueryRelationsFromQueryTree(const std::string_view query_tree) {
  std::istringstream input{std::string(query_tree)};
  std::vector<std::string> lines;
  for (std::string line; std::getline(input, line);) {
    lines.push_back(line);
  }

  auto indent_of = [](const std::string& line) {
    size_t i = 0;
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i]))) {
      ++i;
    }
    return i;
  };

  std::vector<InSubqueryRelationGuess> guesses;
  const std::regex fn_in_regex(R"(function_name:\s*(notIn|in)\b)", std::regex::icase);
  const std::regex col_regex(R"(column_name:\s*([A-Za-z_][A-Za-z0-9_]*))", std::regex::icase);
  const std::regex table_regex(R"(table_name:\s*([A-Za-z0-9_\.]+))", std::regex::icase);
  const std::regex projection_col_line(R"(^\s*([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z0-9_\(\), ]+\s*$)");

  for (size_t i = 0; i < lines.size(); ++i) {
    std::smatch fn_match;
    if (!std::regex_search(lines[i], fn_match, fn_in_regex)) {
      continue;
    }

    const auto fn_indent = indent_of(lines[i]);
    std::string lhs;
    std::string rhs;
    std::string table;
    bool expect_projection_col = false;
    size_t projection_indent = 0;

    for (size_t j = i + 1; j < lines.size(); ++j) {
      const auto current_indent = indent_of(lines[j]);
      if (current_indent <= fn_indent) {
        break;
      }

      std::smatch m;
      if (lhs.empty() && std::regex_search(lines[j], m, col_regex)) {
        lhs = cleanExpression(m[1].str());
      }
      if (lines[j].find("PROJECTION COLUMNS") != std::string::npos) {
        expect_projection_col = true;
        projection_indent = current_indent;
        continue;
      }
      if (expect_projection_col && current_indent > projection_indent && rhs.empty() &&
          std::regex_match(lines[j], m, projection_col_line)) {
        rhs = cleanExpression(m[1].str());
        expect_projection_col = false;
      }
      if (table.empty() && std::regex_search(lines[j], m, table_regex)) {
        table = cleanExpression(m[1].str());
      }
    }

    if (lhs.empty() || rhs.empty() || table.empty()) {
      continue;
    }
    guesses.push_back(InSubqueryRelationGuess{
      .lhs_key = to_upper(lhs),
      .rhs_expression = rhs,
      .rhs_table = table,
    });
  }

  return guesses;
}

std::vector<CorrelatedExistsRelationGuess> guessCorrelatedExistsRelationsFromQueryTree(const std::string_view query_tree) {
  const std::string text(query_tree);
  std::vector<CorrelatedExistsRelationGuess> guesses;

  std::map<std::string, std::string> table_name_by_id;
  {
    const std::regex table_line(R"(TABLE\s+id:\s*(\d+)[^\n]*table_name:\s*([A-Za-z0-9_\.]+))", std::regex::icase);
    std::sregex_iterator begin(text.begin(), text.end(), table_line);
    std::sregex_iterator end;
    for (auto it = begin; it != end; ++it) {
      table_name_by_id[(*it)[1].str()] = cleanExpression((*it)[2].str());
    }
  }

  // We only need correlated equals predicates used by EXISTS/NOT EXISTS subqueries.
  const std::regex equals_block(
      R"(function_name:\s*equals[\s\S]*?column_name:\s*([A-Za-z_][A-Za-z0-9_]*)[^\n]*source_id:\s*(\d+)[\s\S]*?column_name:\s*([A-Za-z_][A-Za-z0-9_]*)[^\n]*source_id:\s*(\d+))",
      std::regex::icase);
  std::sregex_iterator begin(text.begin(), text.end(), equals_block);
  std::sregex_iterator end;
  for (auto it = begin; it != end; ++it) {
    const auto left_column = cleanExpression((*it)[1].str());
    const auto left_source_id = (*it)[2].str();
    const auto right_column = cleanExpression((*it)[3].str());
    const auto right_source_id = (*it)[4].str();
    if (!table_name_by_id.contains(left_source_id) || !table_name_by_id.contains(right_source_id)) {
      continue;
    }
    const auto left_table = table_name_by_id.at(left_source_id);
    const auto right_table = table_name_by_id.at(right_source_id);
    if (left_table.empty() || right_table.empty() || left_column.empty() || right_column.empty()) {
      continue;
    }
    guesses.push_back(CorrelatedExistsRelationGuess{
      .left_table = left_table,
      .left_column = left_column,
      .right_table = right_table,
      .right_column = right_column,
    });
  }

  return guesses;
}

} // namespace sql::clickhouse
