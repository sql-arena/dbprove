#include "row.h"
#include <dbprove/sql/sql_exceptions.h>


namespace sql::odbc {
SqlVariant Row::get(const size_t index) const {
  return result_.get(index);
}

ColumnCount Row::columnCount() const {
  return result_.columnCount();
}
}