#pragma once
#include <memory>
#include "row_iterator.h"
#include "row_base.h"


namespace sql {
class RowBase;
class ColumnBase;
class RowIterable;
class ColumnIterator;

class ResultBase : public std::enable_shared_from_this<ResultBase> {
public:
  virtual ~ResultBase() = default;
  virtual RowCount rowCount() const = 0;
  virtual ColumnCount columnCount() const = 0;
  RowIterable rows();
  /// @brief Get the type of a column by its index
  SqlTypeKind columnType(size_t index) const;
  /**
   * For engines supporting multiple results in a single roundtrip, call this
   *
   * @return this, if more results, nullptr if not
   */
  virtual ResultBase* nextResult() {
    return nullptr;
  }

  /**
   * Iterate through this results until the end.
   * Needed for cases where we want to call nextResult or for the drivers where
   * the rowCount is not available until all rows have been spooled
   */
  void drain();

protected:
  /// @brief return the next row or nullptr if no more rows
  virtual const RowBase& nextRow() = 0;
  /// @brief Reset result cursor
  virtual void reset() {
  };

  friend class RowIterator;
  friend class ColumnIterator;
  std::vector<SqlTypeKind> columnTypes_;
};


/// @brief Sentinel to mark end of row iteration
class SentinelResult final : public ResultBase {
protected:
  const RowBase& nextRow() override {
    return SentinelRow::instance();
  }

  size_t rowCount() const override {
    return 0;
  }

  size_t columnCount() const override {
    return 0;
  }
};
}