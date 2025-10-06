#include "row.h"
#include <dbprove/sql/sql.h>
#include "result.h"
#include "dbprove/sql/sql_exceptions.h"

namespace sql::databricks {
Row::~Row() {
  if (ownsResult_) {
    delete data_;
  }
}

SqlVariant Row::get(size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(columnCount()) + " columns are available.");
  }

  return data_->rows_[currentRow_][index];
}

ColumnCount Row::columnCount() const {
  return data_->columnCount();
}
}