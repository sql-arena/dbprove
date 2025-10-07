#include "row_iterator.h"
#include "result_base.h"

#include "sql_exceptions.h"

namespace sql {
RowIterable ResultBase::rows() {
  reset(); // Reset the cursor position before creating iterator
  return RowIterable(*this);
}

SqlTypeKind ResultBase::columnType(const size_t index) const {
  if (index > columnTypes_.size()) {
    throw InvalidColumnsException("Attempted to access type of column at index " + std::to_string(index));
  }
  return columnTypes_[index];
}

void ResultBase::drain() {
  for (auto& _ : rows()) {
    // Do nothing
  }
}
}