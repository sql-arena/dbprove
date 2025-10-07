#include "row.h"
#include "sql_exceptions.h"
#include "result.h"

namespace sql::mariadb {
SqlVariant Row::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException(
        "Attempted to access column at index " + std::to_string(index) + " but only " + std::to_string(columnCount()) +
        " columns are available.");
  }

  const auto type = result_->columnType(index);
  const char* data = result_->columnData(index);
  /* nullptr represents a NULL, despite type
   * Everything else is a string representation of the data
   */
  if (!data) {
    return SqlVariant();
  }
  switch (type) {
    case SqlTypeKind::SMALLINT:
      return SqlVariant(static_cast<int16_t>(std::stoi(data)));
    case SqlTypeKind::INT:
      return SqlVariant(static_cast<int32_t>(std::stol(data)));
    case SqlTypeKind::BIGINT:
      return SqlVariant(std::stoll(data));
    case SqlTypeKind::REAL:
    case SqlTypeKind::DOUBLE:
      return SqlVariant(std::stod(data));
    case SqlTypeKind::DECIMAL:
      return SqlVariant(data);
    case SqlTypeKind::STRING:
      return SqlVariant(data);
    default:
      throw InvalidTypeException("MySQL driver could not parse type: " + std::string(to_string(type)));
  }
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}
}