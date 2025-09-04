#include "row.h"
#include "sql_exceptions.h"

#include <clickhouse/client.h>

namespace ch = clickhouse;

namespace sql::clickhouse {
SqlVariant Row::get(size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(columnCount()) + " columns are available.");
  }

  return result_->getRowValue(index);
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}

Row::Row(Result* result)
  : result_(result) {
}
}