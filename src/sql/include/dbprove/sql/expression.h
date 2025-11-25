#pragma once

namespace sql {
using ssize_t = std::make_signed_t<size_t>;

/**
 * Represents a parsed token from an expression
 */
struct Token {
  enum class Type {
    Literal, // Represents variables or numbers
    Operator, // Represents +, -, *, /
    LeftParen, // (
    RightParen, // )
    Comma, // ,
    LiteralString,
    Function,
    ExtractFunction,
    CountDistinctFunction,
    OperatorFunction, // ClickHouse (and likely others) turn some operators into function calls.
    None,
    Ignore
  };

  Type type;
  std::string value;
  int matching = 0;

  bool isFunction() const {
    return type == Type::Function || type == Type::ExtractFunction || type == Type::CountDistinctFunction || type ==
           Type::OperatorFunction;
  }
};

Token safeToken(const std::vector<Token>& tokens, ssize_t index);

std::vector<Token> tokenize(const std::string& expr);
std::string render(std::vector<Token>& tokens);
/**
 * Engines can construct one of these to do special, engine specific rendering
 */
struct EngineDialect {
  static const std::map<std::string_view, std::string_view>& ansiFunctions();

  virtual ~EngineDialect() = default;
  /**
   * Do something special before rendering the expression by modifying the token stream directly.
   * @param tokens
   */
  virtual void preRender(std::vector<Token>& tokens) {
  };

  virtual std::string postRender(std::string toRender) {
    return std::move(toRender);
  }

  std::map<std::string_view, std::string_view> functions() const {
    auto map = engineFunctions();
    const auto& ansi = ansiFunctions();
    map.insert(ansi.begin(), ansi.end());
    return map;
  }

protected:
  /**
   * The special functions for the engine along with their ANSI SQL equivalents (if available)
   * @return The key is the function names unique to this engine, the value is their ANSI equivalent.
   * If no ANSI equivalent exists, an empty string is the value.
   */
  virtual const std::map<std::string_view, std::string_view>& engineFunctions() const;
};

/**
 * RAII setting the dialect
 */
struct DialectContext {
  explicit DialectContext(EngineDialect& dialect);
  ~DialectContext();
};

/**
 * Cleans up an expression by removing unnecessary whitespace, newlines, tabs etc.
 * @param expression Expression to clean
 * @return A better formatted expression
 */
std::string cleanExpression(std::string expression);
}