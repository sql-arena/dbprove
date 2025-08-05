#include "row.h"

#include "sql_exceptions.h"

sql::SqlVariant sql::boilerplate::Row::get(size_t index) const {
  if (index < columnCount()) {
    throw InvalidColumnsException("Attemped to access column at index " + std::to_string(index) +
      " but only " + std::to_string(columnCount()) + " columns are available.");
  }
  // TODO: Implement parsing of the row data from whatever memory format the driver supplies
  return SqlVariant();
}

sql::ColumnCount sql::boilerplate::Row::columnCount() const {
  // TODO: Implement the column counting
  return 0;
}