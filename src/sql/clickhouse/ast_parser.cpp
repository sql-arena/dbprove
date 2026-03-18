#include "ast_parser.h"

#include <regex>
#include <sstream>

#include "connection.h"
#include "literals.h"
#include <dbprove/common/string.h>

namespace sql::clickhouse {
namespace {
std::string parseAstTupleLiteral(std::string tuple_values) {
  tuple_values = std::regex_replace(tuple_values, std::regex(R"(\\')"), "'");
  tuple_values = stripClickHouseTypedLiterals(std::move(tuple_values));
  tuple_values = std::regex_replace(tuple_values, std::regex("\\s+"), " ");
  return "(" + tuple_values + ")";
}

std::vector<std::pair<int, std::string>> parseAstLines(const std::string_view ast_explain) {
  std::vector<std::pair<int, std::string>> lines;
  std::istringstream input{std::string(ast_explain)};
  std::string line;
  while (std::getline(input, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    size_t indent = 0;
    while (indent < line.size() && line[indent] == ' ') {
      ++indent;
    }
    lines.emplace_back(static_cast<int>(indent), line);
  }
  return lines;
}
} // namespace

std::map<std::string, std::string> guessSetsFromAst(const std::string_view ast_explain,
                                                    const EngineDialect* dialect) {
  std::map<std::string, std::string> guessed_sets;
  const auto lines = parseAstLines(ast_explain);

  const std::regex in_function(R"(^\s*Function\s+(in|notIn)\b)", std::regex::icase);
  const std::regex function_line(R"(^\s*Function\s+([A-Za-z_][A-Za-z0-9_]*)\b)");
  const std::regex identifier_line(R"(^\s*Identifier\s+(.+)$)");
  const std::regex literal_line(R"(^\s*Literal\s+(.+)$)");
  const std::regex tuple_line(R"(^\s*Literal\s+Tuple_\((.*)\)\s*$)");

  for (size_t i = 0; i < lines.size(); ++i) {
    const auto& [base_indent, base_line] = lines[i];
    if (!std::regex_search(base_line, in_function)) {
      continue;
    }

    std::string identifier;
    std::string tuple_values;
    std::string lhs_function;
    int lhs_function_indent = -1;
    std::vector<std::string> lhs_function_args;
    int left_function_indent = -1;
    std::string left_function_identifier;
    std::string left_function_literal;
    std::string left_function_expression;

    auto finalize_lhs_function = [&]() {
      if (identifier.empty() && !lhs_function.empty()) {
        std::string rendered = lhs_function + "(";
        for (size_t arg_i = 0; arg_i < lhs_function_args.size(); ++arg_i) {
          if (arg_i > 0) {
            rendered += ",";
          }
          rendered += lhs_function_args[arg_i];
        }
        rendered += ")";
        identifier = rendered;
      }
      lhs_function.clear();
      lhs_function_indent = -1;
      lhs_function_args.clear();
    };
    auto finalize_left_function = [&]() {
      if (!left_function_identifier.empty() && !left_function_literal.empty()) {
        left_function_expression = "LEFT(" + left_function_identifier + "," + left_function_literal + ")";
      }
      left_function_indent = -1;
    };

    for (size_t j = i + 1; j < lines.size(); ++j) {
      const auto& [child_indent, child_line] = lines[j];
      if (child_indent <= base_indent) {
        break;
      }
      if (lhs_function_indent >= 0 && child_indent <= lhs_function_indent) {
        finalize_lhs_function();
      }
      if (left_function_indent >= 0 && child_indent <= left_function_indent) {
        finalize_left_function();
      }

      std::smatch m;
      if (std::regex_search(child_line, m, function_line) && to_upper(m[1].str()) == "LEFT") {
        left_function_indent = child_indent;
        left_function_identifier.clear();
        left_function_literal.clear();
        continue;
      }
      if (left_function_indent >= 0 && child_indent > left_function_indent) {
        if (left_function_identifier.empty() && std::regex_match(child_line, m, identifier_line)) {
          left_function_identifier = trim_string(m[1].str());
          continue;
        }
        if (left_function_literal.empty() && std::regex_match(child_line, m, literal_line)) {
          auto literal = trim_string(m[1].str());
          if (!literal.starts_with("Tuple_(")) {
            left_function_literal = stripClickHouseTypedLiterals(std::move(literal));
          }
          continue;
        }
      }
      if (identifier.empty() && lhs_function.empty() && std::regex_search(child_line, m, function_line)) {
        const auto fn_name = m[1].str();
        const auto fn_upper = to_upper(fn_name);
        if (fn_upper != "IN" && fn_upper != "NOTIN") {
          lhs_function = fn_name;
          lhs_function_indent = child_indent;
          continue;
        }
      }
      if (lhs_function_indent >= 0 && child_indent > lhs_function_indent) {
        if (std::regex_match(child_line, m, identifier_line)) {
          lhs_function_args.push_back(m[1].str());
          continue;
        }
        if (std::regex_match(child_line, m, literal_line)) {
          auto literal = m[1].str();
          if (!literal.starts_with("Tuple_(")) {
            lhs_function_args.push_back(stripClickHouseTypedLiterals(std::move(literal)));
          }
          continue;
        }
      }
      if (identifier.empty() && std::regex_match(child_line, m, identifier_line)) {
        identifier = m[1].str();
        continue;
      }
      if (tuple_values.empty() && std::regex_match(child_line, m, tuple_line)) {
        tuple_values = m[1].str();
      }
    }
    finalize_lhs_function();
    finalize_left_function();
    if (!left_function_expression.empty()) {
      identifier = left_function_expression;
    }

    if (identifier.empty() || tuple_values.empty()) {
      continue;
    }

    const auto normalized = parseAstTupleLiteral(tuple_values);
    const auto key = to_upper(cleanExpression(identifier, dialect));
    if (guessed_sets.contains(key)) {
      continue;
    }
    guessed_sets[key] = normalized;
  }

  return guessed_sets;
}

ScopedAstAliases guessScopedAliasesFromAst(const std::string_view ast_explain) {
  ScopedAstAliases aliases;
  const auto lines = parseAstLines(ast_explain);
  const std::regex table_identifier_line(
    R"(^\s*TableIdentifier\s+([A-Za-z0-9_\.]+)(?:\s+\(alias\s+([A-Za-z0-9_]+)\))?\s*$)",
    std::regex::icase);
  const std::regex select_query_line(R"(^\s*SelectQuery\b)");

  struct AstStackFrame {
    int indent = 0;
    bool is_select_query = false;
  };
  std::vector<AstStackFrame> stack;
  int select_query_depth = 0;

  for (const auto& [indent, line] : lines) {
    while (!stack.empty() && stack.back().indent >= indent) {
      if (stack.back().is_select_query) {
        --select_query_depth;
      }
      stack.pop_back();
    }

    const auto is_select_query = std::regex_search(line, select_query_line);
    if (is_select_query) {
      ++select_query_depth;
    }
    stack.push_back(AstStackFrame{
      .indent = indent,
      .is_select_query = is_select_query,
    });

    std::smatch match;
    if (!std::regex_match(line, match, table_identifier_line)) {
      continue;
    }
    const auto table_name = cleanExpression(match[1].str());
    if (table_name.empty()) {
      continue;
    }
    std::string alias;
    if (match.size() > 2) {
      alias = cleanExpression(match[2].str());
    }
    const auto scoped_depth = std::max(0, select_query_depth - 1);
    aliases[scoped_depth][to_lower(table_name)].push_back(alias);
  }
  return aliases;
}

std::string fetchClickHouseExplainAst(Connection& connection,
                                      const std::string_view statement,
                                      const std::string_view artifact_name,
                                      const std::optional<std::string>& cached_ast) {
  if (cached_ast.has_value()) {
    return *cached_ast;
  }

  const std::string explain_ast_stmt = "EXPLAIN AST\n"
                                       + std::string(statement) + "\nFORMAT TSVRaw";
  auto ast_result = connection.fetchAll(explain_ast_stmt);
  std::string string_ast;
  for (auto& row : ast_result->rows()) {
    string_ast += row[0].asString();
    string_ast += "\n";
  }
  connection.storeArtefact(artifact_name, "ast", string_ast);
  return string_ast;
}

void fetchAstAndGuessSets(Connection& connection,
                         const std::string_view statement,
                         const std::string_view artifact_name,
                         const std::optional<std::string>& cached_ast,
                         std::map<std::string, std::string>& guessed_sets,
                         ScopedAstAliases* scoped_aliases,
                         const EngineDialect* dialect,
                         const bool force_fetch_ast) {
  if (!force_fetch_ast && cached_ast.has_value()) {
    guessed_sets = guessSetsFromAst(*cached_ast, dialect);
    if (scoped_aliases != nullptr) {
      *scoped_aliases = guessScopedAliasesFromAst(*cached_ast);
    }
    return;
  }

  const auto string_ast = fetchClickHouseExplainAst(connection, statement, artifact_name, cached_ast);
  guessed_sets = guessSetsFromAst(string_ast, dialect);
  if (scoped_aliases != nullptr) {
    *scoped_aliases = guessScopedAliasesFromAst(string_ast);
  }
}
} // namespace sql::clickhouse
