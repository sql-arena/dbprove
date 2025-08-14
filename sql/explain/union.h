#pragma once

#include "column.h"
#include "node.h"
#include "sql_exceptions.h"

namespace sql::explain {
class Union : public Node {
  static constexpr const char* symbol_ = "∪";

public:
  enum class Type {
    DISTINCT,
    ALL
  };
  explicit Union(const Type type): Node(NodeType::UNION), type(type) {
  }

  std::string compactSymbolic() const override {
    auto result = std::string(symbol_) + "{";
    if (type == Type::DISTINCT) {
      result+= "distinct";
    }
    else {
      result+= "all";
    }

    result += "}";
    return result;
  }
  std::string renderMuggle() const override {
    std::string result = "UNION ";
    if (type == Type::DISTINCT) {
      result+= "DISTINCT";
    }
    else {
      result+= "ALL";
    }
    return result;
  }


  Type type;
};
}