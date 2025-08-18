#include "sort.h"

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
  result += Column::join(columns_sorted, ", ", true);
  return result;
}
}