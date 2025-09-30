#include "row.h"
#include "sql_exceptions.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint.h>
#endif
#include <sqlext.h>


namespace sql::msodbc {
void check(const SQLRETURN result) {
  switch (result) {
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
      return;
    default:
      throw InvalidTypeException("Failed to execute parse typet.");
  }
}

SqlVariant Row::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(columnCount()) + " columns are available.");
  }

  const auto h = static_cast<SQLHSTMT>(handle_);
  const auto i = static_cast<SQLUSMALLINT>(index);
  const auto type_kind = result_->columnType(index);
  switch (type_kind) {
    case SqlTypeKind::TINYINT: {
      int8_t value;
      check(SQLGetData(h, i, SQL_C_CHAR, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::SMALLINT: {
      int16_t value;
      check(SQLGetData(h, i, SQL_C_SHORT, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::INT: {
      int32_t value;
      check(SQLGetData(h, i, SQL_C_LONG, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::BIGINT: {
      int64_t value = 0;
      check(SQLGetData(h, i, SQL_C_SBIGINT, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::REAL: {
      float value = 0.0;
      check(SQLGetData(h, i, SQL_C_FLOAT, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::DOUBLE: {
      double value = 0.0;
      check(SQLGetData(h, i, SQL_C_DOUBLE, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::DECIMAL: {
      check(SQLGetData(h, i, SQL_CHAR, buffer_, sizeof(buffer_), nullptr));
      return static_cast<SqlVariant>(SqlDecimal(std::string(buffer_)));
    }
    case SqlTypeKind::STRING: {
      check(SQLGetData(h, i, SQL_CHAR, buffer_, sizeof(buffer_), nullptr));
      return SqlVariant(std::string(buffer_));
    }
    case SqlTypeKind::SQL_NULL:
      return SqlVariant();
  }
  throw InvalidTypeException("Unsupported type." + std::string(to_string(type_kind)));
}

ColumnCount Row::columnCount() const {
  return result_->columnCount();
}
}