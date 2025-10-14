#include "row.h"
#include "result.h"
#include "sql_exceptions.h"

namespace sql::postgresql {
Row::Row(Result& result)
  : result_(result) {
}

Row::~Row() {
}

ColumnCount Row::columnCount() const {
  return result_.columnCount();
}


SqlVariant Row::get(const size_t index) const {
  return result_.get(index);
}
}