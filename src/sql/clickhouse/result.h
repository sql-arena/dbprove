#pragma once
#include "result_base.h"
#include <vector>
#include <memory>

namespace sql::clickhouse {
class Connection;
class BlockHolder;

class Result : public ResultBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  RowCount currentRowIndex_ = 0;
  SqlVariant getRowValue(size_t index) const;

public:
  explicit Result(std::unique_ptr<BlockHolder> holder);
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  ~Result() override;

protected:
  const RowBase& nextRow() override;
  friend class Connection;
  friend class Row;
};
}