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

namespace sql::odbc {
void check(const SQLRETURN result, SQLHSTMT handle) {
  switch (result) {
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
      return;
    default: {
      std::string error_message = "Failed to execute parse type.";
      if (handle) {
        SQLCHAR sql_state[6] = {0};
        SQLINTEGER native_error = 0;
        SQLCHAR message_text[256] = {0};
        SQLSMALLINT text_length = 0;
        if (SQLGetDiagRec(SQL_HANDLE_STMT, handle, 1, sql_state, &native_error, message_text, sizeof(message_text),
                          &text_length) == SQL_SUCCESS) {
          error_message += " [SQLState: ";
          error_message += reinterpret_cast<char*>(sql_state);
          error_message += "] ";
          error_message += reinterpret_cast<char*>(message_text);
        }
      }
      throw InvalidTypeException(error_message);
    }
  }
}


class Result::Pimpl {
public:
  SQLHSTMT handle_;
  ColumnCount columnCount_;
  std::unique_ptr<Row> currentRow_;
  RowCount currentRowIndex_ = 0;
  std::vector<SqlVariant> rowData_; ///< ODBC requires us to read columns in order. Materialize here.
  std::vector<SQLSMALLINT> columnTypes_;
  Result& result_;

  explicit Pimpl(void* handle, Result* result)
    : handle_(handle)
    , currentRow_(std::make_unique<Row>(*result))
    , result_(*result) {
    initColumnCount();
  }

  char bounceBuffer_[SqlType::MAX_STRING_LENGTH]; ///< Bounce buffer during parse

  void initColumnCount() {
    SQLSMALLINT col_count = 0;
    SQLNumResultCols(handle_, &col_count);
    columnCount_ = col_count;
  }

  SqlVariant odbc2SqlVariant(size_t index) {
    const auto h = handle_;
    const auto i = static_cast<SQLUSMALLINT>(index + 1); // 1-based array
    const auto type_kind = result_.columnType(index);
    SQLLEN indicator = 0;
    switch (type_kind) {
      case SqlTypeKind::SMALLINT: {
        int16_t value = 0;
        check(SQLGetData(h, i, SQL_C_SHORT, &value, sizeof(value), &indicator), h);
        return SqlVariant(value);
      }
      case SqlTypeKind::INT: {
        int32_t value = 0;
        check(SQLGetData(h, i, SQL_C_LONG, &value, sizeof(value), &indicator), h);
        return SqlVariant(value);
      }
      case SqlTypeKind::BIGINT: {
        int64_t value = 0;
        check(SQLGetData(h, i, SQL_C_SBIGINT, &value, sizeof(value), &indicator), h);
        return SqlVariant(value);
      }
      case SqlTypeKind::REAL: {
        float value = 0.0;
        check(SQLGetData(h, i, SQL_C_FLOAT, &value, sizeof(value), &indicator), h);
        return SqlVariant(value);
      }
      case SqlTypeKind::DOUBLE: {
        double value = 0.0;
        check(SQLGetData(h, i, SQL_C_DOUBLE, &value, sizeof(value), &indicator), h);
        return SqlVariant(value);
      }
      case SqlTypeKind::DECIMAL: {
        check(SQLGetData(h, i, SQL_CHAR, bounceBuffer_, sizeof(bounceBuffer_), &indicator), h);
        return static_cast<SqlVariant>(SqlDecimal(std::string(bounceBuffer_)));
      }
      case SqlTypeKind::STRING: {
        check(SQLGetData(h, i, SQL_CHAR, bounceBuffer_, sizeof(bounceBuffer_), &indicator), h);
        return SqlVariant(std::string(bounceBuffer_));
      }
      case SqlTypeKind::SQL_NULL:
        return SqlVariant();
    }
    throw InvalidTypeException("Unsupported type." + std::string(to_string(type_kind)));
  };

  void parseRow() {
    if (currentRowIndex_ == 0) {
      rowData_.clear();
      for (size_t i = 0; i < columnCount_; ++i) {
        rowData_.push_back(odbc2SqlVariant(i));
      }
    } else {
      // Reuse the buffer
      for (size_t i = 0; i < columnCount_; ++i) {
        rowData_[i] = odbc2SqlVariant(i);
      }
    }
  };
};


void Result::initResult(void* handle) {
  static std::map<SQLSMALLINT, SqlTypeKind> type_map = {{SQL_VARCHAR, SqlTypeKind::STRING},
                                                        {SQL_WCHAR, SqlTypeKind::STRING},
                                                        {SQL_WLONGVARCHAR, SqlTypeKind::STRING},
                                                        {SQL_WVARCHAR, SqlTypeKind::STRING},
                                                        {SQL_SMALLINT, SqlTypeKind::SMALLINT},
                                                        {SQL_TINYINT, SqlTypeKind::SMALLINT},
                                                        {SQL_INTEGER, SqlTypeKind::INT},
                                                        {SQL_BIGINT, SqlTypeKind::BIGINT},
                                                        {SQL_REAL, SqlTypeKind::REAL},
                                                        {SQL_DOUBLE, SqlTypeKind::DOUBLE},
                                                        {SQL_FLOAT, SqlTypeKind::DOUBLE},
                                                        {SQL_DECIMAL, SqlTypeKind::DECIMAL},
                                                        {SQL_TYPE_TIME, SqlTypeKind::TIME},
                                                        {SQL_TYPE_DATE, SqlTypeKind::DATE},
                                                        {SQL_TYPE_TIMESTAMP, SqlTypeKind::DATETIME}};
  columnTypes_.clear();
  impl_->initColumnCount();
  const auto column_count = columnCount();
  impl_->currentRowIndex_ = 0;
  for (SQLUSMALLINT i = 1; i <= column_count; ++i) {
    SQLSMALLINT dataType = 0;
    const auto ret = SQLDescribeCol(handle, i, nullptr, 0, nullptr, &dataType, nullptr, nullptr, nullptr);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      if (!type_map.contains(dataType)) {
        throw InvalidTypeException("Unsupported Type in ODBC driver. The Type Enum is: " + std::to_string(dataType));
      }
      columnTypes_.push_back(type_map[dataType]);
    } else {
      const std::string error_message = "Failed to describe column " + std::to_string(i) + " of result set";
      throw InvalidTypeException(error_message);
    }
  }
}

SqlVariant Result::get(size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException(
        "Attempted to access column at index " + std::to_string(index) + " but only " + std::to_string(columnCount()) +
        " columns are available.");
  }
  return impl_->rowData_[index];
}

Result::Result(void* handle)
  : impl_(std::make_unique<Pimpl>(handle, this)) {
  initResult(handle);
}

Result::~Result() {
  SQLFreeHandle(SQL_HANDLE_STMT, impl_->handle_);
}

RowCount Result::rowCount() const {
  return impl_->currentRowIndex_;
}

ColumnCount Result::columnCount() const {
  return impl_->columnCount_;
}

ResultBase* Result::nextResult() {
  if (SQLMoreResults(impl_->handle_) == SQL_NO_DATA) {
    return nullptr;
  }
  initResult(impl_->handle_);
  return this;
}


const RowBase& Result::nextRow() {
  const auto ret = SQLFetch(impl_->handle_);
  switch (ret) {
    case SQL_NO_DATA:
      return SentinelRow::instance();
    case SQL_SUCCESS:
    case SQL_SUCCESS_WITH_INFO:
      impl_->parseRow();
      ++impl_->currentRowIndex_;
      return *impl_->currentRow_;
    default:
      throw std::runtime_error(
          "Failed to fetch row next row from SQL Server. " "The last row was index: " + std::to_string(
              impl_->currentRowIndex_));
  }
}
}