#include "sort.h"
#include "glyphs.h"
#include "sql_exceptions.h"

static constexpr const char* symbol_ = "τ";
static constexpr const char* asc_ = "↑";
static constexpr const char* desc_ = "↓";

namespace sql::explain {
std::string Sort::compactSymbolic() const {
  auto result = std::string(symbol_) + "{";

  for (auto& column : columns_sorted) {
    result += column.name;
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

std::string Sort::renderMuggle(size_t max_width) const {
  std::string result = "SORT ";
  result += join(columns_sorted, ", ", max_width);
  return result;
}

std::string Sort::treeSQLImpl(size_t indent) const {
  std::string result = "(SELECT * ";
  result += newline(indent);
  result += "FROM " + firstChild()->treeSQL(indent + 1);
  result += newline(indent);
  result += "ORDER BY " + join(columns_sorted, ", ");
  result += newline(indent);
  result += ") AS sort_" + nodeName();
  return result;
}
}