#include "row.h"
#include "result.h"
#include <dbprove/sql/sql.h>


namespace sql::duckdb {
Row::~Row() {
}

ColumnCount Row::columnCount() const {
  return result_.columnCount();
}

RowCount Row::rowNumber() const {
  return result_.rowNumber();
}


SqlVariant Row::get(const size_t index) const {
  return result_.get(index);
}
} // namespace:: sql::duckdb