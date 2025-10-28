#include "sql.h"
#include <regex>
#include <set>
#include <stack>

namespace sql {
enum class TokenType {
  Literal, // Represents variables or numbers
  Operator, // Represents +, -, *, /
  LeftParen, // (
  RightParen, // )
  LiteralString,
  Function,
  None
};

// A token representation
struct Token {
  TokenType type;
  std::string value;
  int matching = 0;
};

void addToken(std::vector<Token>& tokens, const Token& token) {
  tokens.push_back(token);
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

    // COUNT(*)
    if (expr[i] == '*' && prev_token.type == TokenType::LeftParen) {
      addToken(tokens, {TokenType::Literal, std::string(1, expr[i])});
      i++;
      continue;
    }

    // Operators
    std::string op = "";
    static const std::set<char> op_chars = {'<', '>', '!', '=', '~', '/', '*', '+', '-'};
    while (op_chars.contains(expr[i])) {
      op += expr[i++];
    }
    if (op == "!~~") {
      // !~~ is a PostgreSQL'ism for NOT LIKE
      addToken(tokens, {TokenType::Literal, "NOT LIKE"});
      continue;
    }
    if (op == "~~") {
      // ~~ is a PostgreSQL'ism for LIKE
      addToken(tokens, {TokenType::Literal, "LIKE"});
      continue;
    }

    if (!op.empty()) {
      addToken(tokens, {TokenType::Operator, op});
      continue;
    };

    // Literals
    static const std::set<char> valid_literal = {'.', '_', '\"', '[', ']', '#', ',', '@'};
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

    static const std::set<std::string> funcs = {"SUM", "MAX", "MIN", "COUNT", "COUNT_BIG", "BLOOM", "ANY"};
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


std::vector<Token> removeRedundantParenthesis(std::vector<Token>& tokens) {
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
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].type != TokenType::LeftParen) {
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
    auto right_offset = tokens[i].matching;
    if (tokens[right_offset - 1].type != TokenType::RightParen) {
      continue;
    }
    // We now have “((“ followed by “))” later. The outermost parenthesis is redundant.
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

std::string stripDoubleQuotes(std::string expr) {
  if (expr.size() >= 2 && expr.front() == '\"' && expr.back() == '\"') {
    expr = expr.substr(1, expr.size() - 2);
  }
  return expr;
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
      case TokenType::Operator:
        if (next_type == TokenType::Function && tokens[i + 1].value == "IN") {
          // See below for PG oddness
          result += " ";
          continue;
        }
        result += " " + token.value + " ";
        break;
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
      default:
        result += token.value;
        break;
    }
  }
  return result;
}

Table splitTable(std::string_view table_name) {
  const size_t delimiter = table_name.find('.');
  if (delimiter == std::string::npos) {
    throw std::invalid_argument("Table name must be qualified with a schema (e.g., 'schema_name.table_name').");
  }
  auto schema = table_name.substr(0, delimiter);
  auto table = table_name.substr(delimiter + 1);
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
  tokens = removeRedundantParenthesis(tokens);
  return render(tokens);
}
}