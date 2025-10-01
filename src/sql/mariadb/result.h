#pragma once
#include <vector>

#include "result_base.h"

namespace sql::mariadb {
class Row;

class Result final : public ResultBase {
  friend class Row;
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const std::vector<SqlVariant>& currentRow() const;
  RowCount currentRowIndex_ = 0;
  const char* columnData(size_t index) const;

public:
  explicit Result(void* mysql_result);
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  ~Result() override;

protected:
  const RowBase& nextRow() override;
};
}