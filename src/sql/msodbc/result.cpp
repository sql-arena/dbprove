#include "result.h"

#include <map>

#include "row.h"
#include "sql_exceptions.h"
#include <duckdb/common/exception.hpp>
#ifdef _WIN32
#include <windows.h>
#else
#include <cstdint.h>
#endif
#include <sql.h>
#include <sqlext.h>

namespace sql::msodbc {
class Result::Pimpl {
public:
  SQLHSTMT handle_;
  ColumnCount columnCount_;
  std::unique_ptr<Row> currentRow_;
  RowCount currentRowIndex_ = 0;

  std::vector<SQLSMALLINT> columnTypes_;

  explicit Pimpl(void* handle, Result* result)
    : handle_(handle)
    , currentRow_(std::make_unique<Row>(result, handle)) {
    SQLSMALLINT col_count = 0;
    SQLNumResultCols(handle_, &col_count);
    columnCount_ = col_count;
  }
};

void check(const SQLRETURN result) {
  switch (result) {
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
      return;
    default:
      throw InvalidTypeException("Failed to execute parse typet.");
  }
}


SqlVariant Result::odbc2SqlVariant(const size_t index) {
  const auto h = impl_->handle_;
  const auto i = static_cast<SQLUSMALLINT>(index + 1); // 1-based array
  const auto type_kind = columnType(index);
  switch (type_kind) {
    case SqlTypeKind::TINYINT: {
      int8_t value = 0;
      check(SQLGetData(h, i, SQL_C_CHAR, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::SMALLINT: {
      int16_t value = 0;
      check(SQLGetData(h, i, SQL_C_SHORT, &value, sizeof(value), nullptr));
      return SqlVariant(value);
    }
    case SqlTypeKind::INT: {
      int32_t value = 0;
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
      check(SQLGetData(h, i, SQL_CHAR, bounceBuffer_, sizeof(bounceBuffer_), nullptr));
      return static_cast<SqlVariant>(SqlDecimal(std::string(bounceBuffer_)));
    }
    case SqlTypeKind::STRING: {
      check(SQLGetData(h, i, SQL_CHAR, bounceBuffer_, sizeof(bounceBuffer_), nullptr));
      return SqlVariant(std::string(bounceBuffer_));
    }
    case SqlTypeKind::SQL_NULL:
      return SqlVariant();
  }
  throw InvalidTypeException("Unsupported type." + std::string(to_string(type_kind)));
}

void Result::parseRow() {
  if (currentRowIndex_ == 0) {
    for (size_t i = 0; i < columnCount(); ++i) {
      rowData_.push_back(odbc2SqlVariant(i));
    }
  } else {
    // Reuse the buffer
    for (size_t i = 0; i < columnCount(); ++i) {
      rowData_[i] = odbc2SqlVariant(i);
    }
  }
}

Result::Result(void* handle)
  : impl_(std::make_unique<Pimpl>(handle, this)) {
  static std::map<SQLSMALLINT, SqlTypeKind> type_map = {
      {SQL_VARCHAR, SqlTypeKind::STRING},
      {SQL_SMALLINT, SqlTypeKind::SMALLINT},
      {SQL_INTEGER, SqlTypeKind::INT},
      {SQL_BIGINT, SqlTypeKind::BIGINT},
      {SQL_REAL, SqlTypeKind::REAL},
      {SQL_DOUBLE, SqlTypeKind::DOUBLE}
  };

  for (SQLUSMALLINT i = 1; i <= columnCount(); ++i) {
    SQLSMALLINT dataType = 0;
    const auto ret = SQLDescribeCol(
        handle,
        i,
        nullptr, 0, nullptr,
        &dataType,
        nullptr, nullptr, nullptr
        );
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      if (!type_map.contains(dataType)) {
        throw InvalidTypeException("Unsupported ODBC driver type: " + std::to_string(dataType));
      }
      columnTypes_.push_back(type_map[dataType]);
    } else {
      const std::string error_message = "Failed to describe column " + std::to_string(i) + " of result set";
      throw InvalidTypeException(error_message);
    }
  }
}

Result::~Result() {
  SQLFreeHandle(SQL_HANDLE_STMT, impl_->handle_);
}

RowCount Result::rowCount() const {
  return currentRowIndex_;
}

ColumnCount Result::columnCount() const {
  return impl_->columnCount_;
}


const RowBase& Result::nextRow() {
  const auto ret = SQLFetch(impl_->handle_);
  switch (ret) {
    case SQL_NO_DATA:
      return SentinelRow::instance();
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
      parseRow();
      ++currentRowIndex_;
      return *impl_->currentRow_;
    default:
      throw std::runtime_error("Failed to fetch row next row from SQL Server. "
                               "The last row was index: " + std::to_string(currentRowIndex_));
  }
}
}