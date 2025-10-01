#include "row.h"
#include "sql_exceptions.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint.h>
#endif
#include <sqlext.h>


namespace sql::msodbc {
SqlVariant Row::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(columnCount()) + " columns are available.");
  }
  return result_->rowData_[index];
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}
}