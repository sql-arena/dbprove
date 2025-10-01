#include "result.h"
#include "row.h"
#include <sqlite3.h>

#include "sql_exceptions.h"
#include <clickhouse/columns/column.h>
#include <duckdb/common/exception.hpp>

namespace sql::sqlite {
class Result::Pimpl {
public:
  sqlite3_stmt* handle_;
  const ColumnCount columnCount_;
  std::unique_ptr<Row> currentRow_;

  explicit Pimpl(void* handle, Result* result)
    : handle_(static_cast<sqlite3_stmt*>(handle))
    , columnCount_(sqlite3_column_count(handle_))
    , currentRow_(std::make_unique<Row>(result)) {
  }

  ~Pimpl() {
    sqlite3_finalize(handle_);
  }
};

SqlVariant Result::columnData(const size_t index) const {
  const auto sqlite_type = sqlite3_column_type(impl_->handle_, index);
  switch (sqlite_type) {
    case SQLITE3_TEXT:
      return SqlVariant(reinterpret_cast<const char*>(sqlite3_column_text(impl_->handle_, index)));
    case SQLITE_INTEGER:
      return SqlVariant(sqlite3_column_int64(impl_->handle_, index));
    case SQLITE_FLOAT:
      return SqlVariant(sqlite3_column_double(impl_->handle_, index));
    case SQLITE_NULL:
      return SqlVariant();
    default:
      throw InvalidTypeException("Unsupported SQLite type " + std::to_string(sqlite_type));
  }
}

Result::Result(void* handle)
  : impl_(std::make_unique<Pimpl>(handle, this)) {
}

RowCount Result::rowCount() const {
  return currentRowIndex_;
}

ColumnCount Result::columnCount() const {
  return impl_->columnCount_;
}

Result::~Result() {
}


const RowBase& Result::nextRow() {
  auto step_result = sqlite3_step(impl_->handle_);
  if (step_result != SQLITE_ROW) {
    return SentinelRow::instance();
  }
  ++currentRowIndex_;
  return *impl_->currentRow_;
}
}