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

// Function to tokenize the input expression
std::vector<Token> tokenize(const std::string& expr) {
  std::vector<Token> tokens;
  size_t i = 0;
  while (i < expr.size()) {
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
      tokens.push_back({TokenType::LiteralString, literal_string});
      continue;
    }

    // Handle parentheses
    if (expr[i] == '(') {
      tokens.push_back({TokenType::LeftParen, "("});
      i++;
      continue;
    }

    if (expr[i] == ')') {
      tokens.push_back({TokenType::RightParen, ")"});
      i++;
      continue;
    }

    // 2 char operators
    if (i + 1 < expr.size()) {
      std::string op{expr[i], expr[i + 1]};
      static const std::set<std::string> two_char_ops = {"<>", "!=", ">=", "<="};
      if (two_char_ops.contains(op)) {
        tokens.push_back({TokenType::Operator, std::string(1, expr[i]) + std::string(1, expr[i + 1])});
        i += 2; // Advance past both characters
        continue;
      }
    }
    // 1 char operators
    if (std::string("~!+-*/=<>").find(expr[i]) != std::string::npos) {
      tokens.push_back({TokenType::Operator, std::string(1, expr[i])});
      i++;
      continue;
    }

    // Literals
    static const std::set<char> valid_literal = {'.', '_', '\"', '[', ']', '#', ','};
    if (!std::isalnum(expr[i]) && !valid_literal.contains(expr[i])) {
      throw std::runtime_error("Invalid character in expression, expected a literal: " + std::string(1, expr[i]));
    }
    std::string literal;
    while (i < expr.size() && (std::isalnum(expr[i]) || valid_literal.contains(expr[i]))) {
      literal += expr[i++];
    }

    auto upper_literal = to_upper(literal);
    std::regex op_regex(R"(OR|AND|NOT)");
    if (std::regex_match(upper_literal, op_regex)) {
      tokens.push_back({TokenType::Operator, upper_literal});
      continue;
    }

    static const std::set<std::string> funcs = {"SUM", "MAX", "MIN", "COUNT", "COUNT_BIG", "BLOOM"};
    static const std::map<std::string, std::string> translate = {{"COUNT_BIG", "COUNT"}};
    if (funcs.contains(upper_literal)) {
      if (translate.contains(upper_literal)) {
        upper_literal = translate.at(upper_literal);
      }
      tokens.push_back({TokenType::Function, upper_literal});
      continue;
    }

    tokens.push_back({TokenType::Literal, literal});
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
    // We now have (( followed by )) later. THe outermost parenthesis is redundant
    removeMatching(tokens, i);
  }

  if (tokens[0].type == TokenType::LeftParen && tokens[0].matching == tokens.size() - 1) {
    // Entire expressin in paranthesis
    removeMatching(tokens, 0);
  }

  std::vector<Token> optimizedTokens;
  for (auto& token : tokens) {
    if (token.matching >= 0) {
      optimizedTokens.push_back(token);
    }
  }
  return optimizedTokens;
}


std::string render(const std::vector<Token>& tokens) {
  if (tokens.empty()) {
    return "";
  }
  std::string result;
  Token prev = {TokenType::None, ""};
  for (auto& token : tokens) {
    switch (token.type) {
      case TokenType::Operator:
        result += " " + token.value + " ";
        break;
      case TokenType::LiteralString:
        result += "'" + token.value + "'";
        break;
      case TokenType::Literal:
        if (prev.type == TokenType::Literal || prev.type == TokenType::RightParen) {
          result += " ";
        }
        result += token.value;
        break;
      default:
        result += token.value;
        break;
    }
    prev = token;
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