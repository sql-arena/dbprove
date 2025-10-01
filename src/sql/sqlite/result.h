#pragma once
#include <vector>

#include "result_base.h"

namespace sql::sqlite {
class Row;

class Result final : public ResultBase {
  friend class Row;
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const std::vector<SqlVariant>& currentRow() const;
  SqlVariant columnData(size_t index) const;
  RowCount currentRowIndex_ = 0;

public:
  explicit Result(void* handle);
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  ~Result() override;

protected:
  const RowBase& nextRow() override;
};
}