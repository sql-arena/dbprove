#include <iostream>

#include "sql.h"
#include <regex>
#include <set>
#include <stack>

namespace sql {
using ssize_t = std::make_signed_t<size_t>;

std::string stripDoubleQuotes(std::string expr) {
  if (expr.size() >= 2 && expr.front() == '\"' && expr.back() == '\"') {
    expr = expr.substr(1, expr.size() - 2);
  }
  return expr;
}

enum class TokenType {
  Literal, // Represents variables or numbers
  Operator, // Represents +, -, *, /
  LeftParen, // (
  RightParen, // )
  Comma, // ,
  LiteralString,
  Function,
  OperatorFunction, // ClickHouse (and likely others) turn some operators into function calls.
  None,
  Ignore
};

// A token representation
struct Token {
  TokenType type;
  std::string value;
  int matching = 0;
};

Token safeToken(const std::vector<Token>& tokens, const ssize_t index) {
  if (index >= tokens.size() || index < 0) {
    return Token{TokenType::None, ""};
  }
  return tokens[index];
}

void addToken(std::vector<Token>& tokens, const Token& token) {
  tokens.push_back(token);
}


auto& operatorFuncs() {
  static const std::map<std::string, std::string> operatorFuncs = {{"greaterOrEquals", ">="},
                                                                   {"greater", ">"},
                                                                   {"multiply", "*"},
                                                                   {"minus", "-"},
                                                                   {"plus", "+"},
                                                                   {"less", "<"},
                                                                   {"lessOrEquals", "<="},
                                                                   {"equals", "="},
                                                                   {"notEquals", "<>"},
                                                                   {"funcAnd", "AND"},
                                                                   {"funcOr", "OR"},
                                                                   {"funcLike", "LIKE"},
                                                                   {"_CAST", "::"}};
  return operatorFuncs;
}

std::string transformPGShittyAny(std::string expr) {
  if (expr.size() >= 2 && expr.front() == '{' && expr.back() == '}') {
    expr = expr.substr(1, expr.size() - 2);
    std::vector<std::string> parts;
    size_t start = 0, end = 0;
    while ((end = expr.find(',', start)) != std::string::npos) {
      parts.push_back("'" + expr.substr(start, end - start) + "'");
      start = end + 1;
    }
    parts.push_back("'" + expr.substr(start) + "'");
    std::string result;
    for (size_t i = 0; i < parts.size(); ++i) {
      if (i > 0)
        result += ",";

      result += stripDoubleQuotes(parts[i]);
    }
    return result;
  }
  return expr;
}


std::string render(const std::vector<Token>& tokens) {
  if (tokens.empty()) {
    return "";
  }

  std::string result;
  for (size_t i = 0; i < tokens.size(); ++i) {
    Token token = tokens[i];
    const auto prev_type = (i > 0) ? tokens[i - 1].type : TokenType::None;
    const auto prev_prev_type = (i > 1) ? tokens[i - 2].type : TokenType::None;
    const auto next_type = (i < tokens.size() - 1) ? tokens[i + 1].type : TokenType::None;
    switch (token.type) {
      case TokenType::Operator: {
        const bool is_cast = token.value == "::";
        result += is_cast ? "" : " ";
        if (next_type == TokenType::Function && tokens[i + 1].value == "IN") {
          // See below for PG oddness
          continue;
        }
        result += token.value;
        result += is_cast ? "" : " ";
        break;
      }
      case TokenType::LiteralString:

        if (prev_type == TokenType::LeftParen && prev_prev_type == TokenType::Function) {
          // PG shitty way of dealing with ANY / IN
          result += transformPGShittyAny(token.value);
        } else {
          result += "'" + token.value + "'";
        }
        break;
      case TokenType::Literal:
        if (prev_type == TokenType::Literal || prev_type == TokenType::RightParen) {
          result += " ";
        }
        result += token.value;
        break;
      case TokenType::Ignore:
        break;
      default:
        result += token.value;
        break;
    }
  }
  return result;
}

// Function to tokenize the input expression
std::vector<Token> tokenize(const std::string& expr) {
  std::vector<Token> tokens;
  Token prev_token = {TokenType::None, ""};
  size_t i = 0;
  while (i < expr.size()) {
    if (!tokens.empty()) {
      prev_token = tokens.back();
    }

    // Skip whitespace
    if (std::isspace(expr[i])) {
      i++;
      continue;
    }

    // String literal
    if (expr[i] == '\'') {
      std::string literal_string;
      while (expr[++i] != '\'') {
        literal_string += expr[i];
      }
      i++;
      addToken(tokens, {TokenType::LiteralString, literal_string});
      continue;
    }

    // Parenthesis
    if (expr[i] == '(') {
      addToken(tokens, {TokenType::LeftParen, "("});
      i++;
      continue;
    }
    if (expr[i] == ')') {
      addToken(tokens, {TokenType::RightParen, ")"});
      i++;
      continue;
    }

    if (expr[i] == ',') {
      addToken(tokens, {TokenType::Comma, ","});
      i++;
      continue;
    }

    // COUNT(*)
    if (expr[i] == '*' && prev_token.type == TokenType::LeftParen) {
      addToken(tokens, {TokenType::Literal, std::string(1, expr[i])});
      i++;
      continue;
    }

    // Operators
    std::string op = "";
    static const std::set<char> op_chars = {'<', '>', '!', '=', '~', '/', '*', '+', '-', ':'};
    while (op_chars.contains(expr[i])) {
      op += expr[i++];
    }
    if (op == "!~~") {
      // !~~ is a PostgreSQL'ism for NOT LIKE
      addToken(tokens, {TokenType::Operator, "NOT LIKE"});
      continue;
    }
    if (op == "~~") {
      // ~~ is a PostgreSQL'ism for LIKE
      addToken(tokens, {TokenType::Operator, "LIKE"});
      continue;
    }
    if (op == "::") {
      // Cast
      addToken(tokens, {TokenType::Operator, "::"});
      continue;
    }

    if (!op.empty()) {
      addToken(tokens, {TokenType::Operator, op});
      continue;
    };

    // Literals
    static const std::set<char> valid_literal = {'.', '_', '\"', '[', ']', '#', '@'};
    if (!std::isalnum(expr[i]) && !valid_literal.contains(expr[i])) {
      throw std::runtime_error(
          "Invalid character in expression, expected a literal, found: '" + std::string(1, expr[i]) + "' in " + expr);
    }

    std::string literal;
    while (i < expr.size() && (std::isalnum(expr[i]) || valid_literal.contains(expr[i]))) {
      literal += expr[i++];
    }

    auto upper_literal = to_upper(literal);
    std::regex op_regex(R"(OR|AND|NOT|LIKE|ILIKE)");
    if (std::regex_match(upper_literal, op_regex)) {
      addToken(tokens, {TokenType::Operator, upper_literal});
      continue;
    }

    /* Clickhouse (and friends) magic functions that are actually operators */
    if (operatorFuncs().contains(literal)) {
      addToken(tokens, {TokenType::OperatorFunction, literal});
      continue;
    }

    static const std::set<std::string> funcs = {"SUM",
                                                "MAX",
                                                "MIN",
                                                "AVG",
                                                "COUNT",
                                                "COUNT_BIG",
                                                "BLOOM",
                                                "ANY",
                                                "EXISTS"};
    static const std::map<std::string, std::string> translate = {{"COUNT_BIG", "COUNT"},
                                                                 // F*cked up PostgreSQL'isms
                                                                 {"\"left\"", "LEFT"},
                                                                 {"\"right\"", "RIGHT"},
                                                                 {"\"substring\"", "SUBSTRING"},
                                                                 // ANY is IN to normal people
                                                                 {"ANY", "IN"}};

    if (funcs.contains(upper_literal)) {
      if (translate.contains(upper_literal)) {
        upper_literal = translate.at(upper_literal);
      }
      addToken(tokens, {TokenType::Function, upper_literal});
      continue;
    }

    addToken(tokens, {TokenType::Literal, literal});
  }

  return tokens;
}

void removeMatching(std::vector<Token>& tokens, size_t index) {
  tokens[tokens[index].matching].matching = -1;
  tokens[index].matching = -1;
}

static size_t matchingParenthesis(const std::vector<Token>& tokens, size_t left_idx) {
  int depth = 1;
  for (size_t k = left_idx + 1; k < tokens.size(); ++k) {
    if (tokens[k].type == TokenType::LeftParen) {
      ++depth;
      continue;
    }
    if (tokens[k].type == TokenType::RightParen) {
      depth--;
    }
    if (depth == 0) {
      return k;
    }
  }
  throw std::runtime_error("Malformed expression: unmatched '(' at token index " + std::to_string(left_idx));
}

static std::vector<Token> processOperatorFunction(const std::vector<Token> tokens) {
  std::vector<Token> out;
  for (size_t i = 0; i < tokens.size(); ++i) {
    const Token& t = tokens[i];

    if (t.type == TokenType::OperatorFunction) {
      if (i + 1 >= tokens.size() || tokens[i + 1].type != TokenType::LeftParen) {
        throw std::runtime_error("Malformed expression: expected '(' after operator function " + t.value);
      }
      // Found a valid OperatorFunction  of form F(exprA, exprB). Find exprA and exprB and
      // transform to exprA <op> exprB
      const auto op = operatorFuncs().at(t.value);

      const size_t left_idx = i + 1;
      const size_t right = matchingParenthesis(tokens, left_idx);

      // Split arguments by top-level commas
      std::vector<std::pair<size_t, size_t>> expr_ranges;
      size_t arg_start = left_idx + 1;
      int depth = 0;
      for (size_t k = left_idx + 1; k < right; ++k) {
        if (tokens[k].type == TokenType::LeftParen) {
          ++depth;
        } else if (tokens[k].type == TokenType::RightParen) {
          --depth;
        }

        if (depth == 0 && tokens[k].type == TokenType::Comma) {
          expr_ranges.emplace_back(arg_start, k);
          arg_start = k + 1;
        }
      }
      expr_ranges.emplace_back(arg_start, right);

      // transform func(arg1, arg2) => (arg1 <op> arg2)
      out.insert(out.end(), tokens.begin(), tokens.begin() + i); // Everything up until the func
      out.push_back({TokenType::LeftParen, "("});
      for (size_t i = 0; i < expr_ranges.size(); ++i) {
        out.insert(out.end(), tokens.begin() + expr_ranges[i].first, tokens.begin() + expr_ranges[i].second);
        if (i < expr_ranges.size() - 1) {
          out.push_back({TokenType::Operator, op});
        }
      }
      out.push_back({TokenType::RightParen, ")"});
      if (right + 1 < tokens.size()) {
        // remaining tokens (if any) after the closing ')'
        out.insert(out.end(), tokens.begin() + right + 1, tokens.end());
      }
      return out;
    }
  }
  return tokens;
}

std::vector<Token> transformOperatorFuncs(std::vector<Token> tokens) {
  if (tokens.empty()) {
    return tokens;
  }
  size_t token_size = 0;
  do {
    token_size = tokens.size();
    tokens = processOperatorFunction(tokens);
  } while (token_size != tokens.size());
  return tokens;
}

std::vector<sql::Token> removeRedundantParenthesis(std::vector<Token>& tokens) {
  if (tokens.empty()) {
    return tokens;
  }
  std::stack<Token*> match;
  for (size_t i = 0; i < tokens.size(); ++i) {
    switch (tokens[i].type) {
      case TokenType::LeftParen:
        match.push(&tokens[i]);
        break;
      case TokenType::RightParen: {
        const auto other = match.top();
        match.pop();
        other->matching = i;
        break;
      }
      default:
        break;
    }
  }
  if (!match.empty()) {
    throw std::runtime_error("Malformed expression: unmatched '('");
  }
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto currentToken = tokens[i];
    if (currentToken.type != TokenType::LeftParen) {
      continue;
    }
    if (tokens[i + 1].type == TokenType::Literal && tokens[i + 2].type == TokenType::RightParen) {
      if (i > 0 && tokens[i - 1].type == TokenType::Function) {
        continue; // Don't remove if part of a larger expression
      }
      // Lonely literal inside parenthesis
      removeMatching(tokens, i);
    }

    if (tokens[i + 1].type != TokenType::LeftParen) {
      continue;
    }
    const auto right_offset = currentToken.matching;
    if (safeToken(tokens, right_offset + 1).type != TokenType::RightParen) {
      continue;
    }
    // We now have “((“ followed by matching “))” later. The innermost parenthesis is redundant.
    removeMatching(tokens, i);
  }

  if (tokens[0].type == TokenType::LeftParen && tokens[0].matching == tokens.size() - 1) {
    // Entire expression in parentheses
    removeMatching(tokens, 0);
  }

  std::vector<Token> optimizedTokens;
  for (const auto& token : tokens) {
    if (token.matching >= 0) {
      addToken(optimizedTokens, token);
    }
  }
  return optimizedTokens;
}


Table splitTable(std::string_view table_name) {
  const size_t delimiter = table_name.find('.');
  if (delimiter == std::string::npos) {
    throw std::invalid_argument("Table name must be qualified with a schema (e.g., 'schema_name.table_name').");
  }
  const auto schema = table_name.substr(0, delimiter);
  const auto table = table_name.substr(delimiter + 1);
  if (schema.empty() || table.empty()) {
    throw std::invalid_argument("Schema and table names cannot be empty.");
  }

  return Table{std::string(schema), std::string(table)};
}

std::string cleanExpression(std::string expression) {
  // XML escape back to the real values
  expression = std::regex_replace(expression, std::regex("&lt;"), "<");
  expression = std::regex_replace(expression, std::regex("&gt;"), ">");

  // DuckDB optional expressions
  expression = std::regex_replace(expression, std::regex("""optional:"), "");
  // SQL Server braces
  expression = std::regex_replace(expression, std::regex("[\\[\\]]"), "");
  // Strip schema from schema.table
  size_t prev_size = 0;
  do {
    prev_size = expression.size();
    expression = std::regex_replace(expression, std::regex(R"((?:[a-zA-Z_]\w*\.)?([a-zA-Z_]\w*))"), "$1");
  } while (expression.size() != prev_size);
  // Remove casts
  expression = std::regex_replace(expression, std::regex(R"(::\w+)"), "");

  auto tokens = tokenize(expression);
  tokens = transformOperatorFuncs(tokens);
  tokens = removeRedundantParenthesis(tokens);
  return render(tokens);
}


std::string removeExpressionFunction(const std::string& expression, const std::string_view function_name) {
  std::string function_name_upper = to_upper(function_name);
  auto tokens = tokenize(expression);
  for (ssize_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].type != TokenType::Function) {
      continue;
    }
    const std::string_view token_func = tokens[i].value;
    if (token_func == function_name_upper) {
      // func()
      tokens[i].type = TokenType::Ignore;
      tokens[i + 1].type = TokenType::Ignore;
      tokens[i + 2].type = TokenType::Ignore;

      // Parenthesis  of form (func()))
      const auto parenthesis_before = safeToken(tokens, i - 1);
      const auto parenthesis_after = safeToken(tokens, i + 3);

      ssize_t func_pos = i - 1;
      if (parenthesis_before.type == TokenType::LeftParen && parenthesis_after.type == TokenType::RightParen) {
        tokens[i - 1].type = TokenType::Ignore;
        tokens[i + 3].type = TokenType::Ignore;
        --func_pos;
      }

      // Operators before the func() (NOT, AND, etc)
      while (true) {
        const auto potential_op = safeToken(tokens, func_pos);
        if (potential_op.type != TokenType::Operator) {
          break;
        }
        tokens[func_pos].type = TokenType::Ignore;
        --func_pos;
      }
    }
  }
  return render(tokens);
}
}