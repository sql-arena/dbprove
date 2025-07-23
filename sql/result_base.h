#pragma once
#include <memory>

#include "row_iterator.h"


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

protected:
  /// @brief return the next row or nullptr if no more rows
  virtual std::unique_ptr<RowBase> nextRow() = 0;
  /// @brief Reset result cursor
  virtual void reset() {
  };

  friend class RowIterator;
  friend class ColumnIterator;
};


/// @brief Sentinel to mark end of row iteration
class SentinelResult : public ResultBase {
protected:
  std::unique_ptr<RowBase> nextRow() override {
    return nullptr;
  }

  size_t rowCount() const override {
    return 0;
  }

  size_t columnCount() const override {
    return 0;
  }
};
}