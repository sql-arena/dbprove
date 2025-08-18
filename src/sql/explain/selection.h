#pragma once
#include <string>

#include "../include/dbprove/sql/explain/node.h"

namespace sql::explain
{
  class Column;

  class Selection : public Node
  {
    static const constexpr char* symbol_ = "σ";

  public:
    Selection(const std::string& filter_expression)
      : Node(NodeType::SELECTION)
      , filter_expression(filter_expression)
    {
    }

    std::string compactSymbolic() const override
    {
      std::string result;
      result += symbol_;
      result += "{";
      result += filter_expression;
      result += "}";
      return result;
    }

    std::string renderMuggle(size_t max_width) const override
    {
      std::string result = "FILTER ";
      result += "(";
      result += filter_expression;
      result += ")";
      return result;
    }

    const std::string filter_expression;
  };
}
