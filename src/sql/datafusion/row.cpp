#include "row.h"

#include "result.h"

namespace sql::datafusion {
SqlVariant Row::get(const size_t index) const {
  return result_->columnData(index);
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}
}
