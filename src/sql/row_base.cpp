#include "include/dbprove/sql/row_base.h"

#include <iostream>


namespace sql {
std::string RowBase::dump() const {
  std::string out = "ColumnCount: " + std::to_string(columnCount()) + "]\n";
  for (size_t i = 0; i < columnCount(); ++i) {
    out += asString(i) + "::";
  }
  return out;
}

std::unique_ptr<MaterialisedRow> RowBase::materialise() const {
  std::vector<SqlVariant> values;
  values.reserve(columnCount());
  for (size_t i = 0; i < columnCount(); ++i) {
    values.push_back(get(i));
  }
  return std::make_unique<MaterialisedRow>(std::move(values));
}
}
