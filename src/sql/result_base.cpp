#include "row_iterator.h"
#include "result_base.h"

#include "sql_exceptions.h"
#include "plog/Log.h"

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

std::string ResultBase::dump()
{
  std::string out = "Total Rows: " + std::to_string(rowCount()) + "\n";
  for (const auto& r : rows()) {
    std::string rowDump = r.dump();
    out += rowDump;
  }
  reset();
  return std::move(out);
}
}
