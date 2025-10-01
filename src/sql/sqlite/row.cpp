#include "row.h"
#include "sql_exceptions.h"
#include "result.h"

namespace sql::sqlite {
SqlVariant Row::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(columnCount()) + " columns are available.");
  }
  return result_->columnData(index);
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}
}