#pragma once

#include "column.h"
#include "node.h"
#include "sql_exceptions.h"

namespace sql::explain {
class Sort : public Node {
  static constexpr const char* symbol_ = "τ";
  static constexpr const char* asc_ = "↑";
  static constexpr const char* desc_ = "↓";

public:
  explicit Sort(const std::vector<Column>& columns_sorted)
    : Node(NodeType::SORT), columns_sorted(columns_sorted) {
  }

  std::string compactSymbolic() const override {
    auto result = std::string(symbol_) + "{";

    for (auto& column : columns_sorted) {
      result+=column.name;
      switch (column.sorting) {
        case Column::Sorting::ASC:
          result += asc_;
          break;
        case Column::Sorting::DESC:
          result += desc_;
          break;
        case Column::Sorting::RANDOM:
          throw InvalidPlanException("Did not expect to find a sort with random sorting");
      }
    }
    result += "}";
    return result;
  }
  std::string renderMuggle() const override {
    std::string result = "SORT ";
    result += Column::join(columns_sorted, ", ", true);
    return result;
  }


  std::vector<Column> columns_sorted;
};
}