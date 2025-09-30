#pragma once
#include <vector>

#include "result_base.h"

namespace sql::msodbc {
class Result final : public ResultBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const std::vector<SqlVariant>& currentRow() const;
  RowCount currentRowIndex_ = 0;

public:
  explicit Result(void* handle);
  ~Result() override;
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;

protected:
  const RowBase& nextRow() override;
};
}