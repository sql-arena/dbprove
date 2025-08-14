#pragma once

#include "explain/column.h"
#include "explain/node.h"
#include "sql_exceptions.h"

namespace sql::explain {
class Union : public Node {


public:
  enum class Type {
    DISTINCT,
    ALL
  };
  explicit Union(const Type type): Node(NodeType::UNION), type(type) {
  }

  std::string compactSymbolic() const override;

  std::string renderMuggle() const override;


  Type type;
};
}