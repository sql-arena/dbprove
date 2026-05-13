#include "result.h"

#include "row.h"
#include "sql_exceptions.h"

namespace sql::datafusion {
Result::Result(std::vector<std::vector<SqlVariant>> rows, std::vector<SqlTypeKind> column_types)
  : rows_(std::move(rows))
  , current_row_(std::make_unique<Row>(this))
  , column_count_(column_types.size()) {
  columnTypes_ = std::move(column_types);
}

Result::~Result() = default;

RowCount Result::rowCount() const {
  return rows_.size();
}

ColumnCount Result::columnCount() const {
  return column_count_;
}

const std::vector<SqlVariant>& Result::currentRow() const {
  if (current_row_index_ == 0 || current_row_index_ > rows_.size()) {
    throw InvalidRowsException("No current DataFusion row is available", "DataFusion result iteration");
  }
  return rows_[current_row_index_ - 1];
}

SqlVariant Result::columnData(const size_t index) const {
  if (index >= column_count_) {
    throw InvalidColumnsException("Attempted to access column at index " + std::to_string(index) +
                                  " but only " + std::to_string(column_count_) + " columns are available.");
  }
  return currentRow()[index];
}

const RowBase& Result::nextRow() {
  if (current_row_index_ >= rows_.size()) {
    return SentinelRow::instance();
  }
  ++current_row_index_;
  return *current_row_;
}

void Result::reset() {
  current_row_index_ = 0;
}
}
