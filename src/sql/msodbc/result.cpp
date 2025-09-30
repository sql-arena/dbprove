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
      throw std::runtime_error("Failed to fetch row next row from SQL Server. "
                               "The last row was index: " + std::to_string(currentRowIndex_));
    default:
      ++currentRowIndex_;
      break;
  }
  return *impl_->currentRow_;
}
}