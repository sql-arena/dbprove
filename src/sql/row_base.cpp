#include "include/dbprove/sql/row_base.h"

namespace sql {
std::unique_ptr<MaterialisedRow> RowBase::materialise() const {
  std::vector<SqlVariant> values;
  values.reserve(columnCount());
  for (size_t i = 0; i < columnCount(); ++i) {
    values.push_back(get(i));
  }
  return std::make_unique<MaterialisedRow>(std::move(values));
}
}