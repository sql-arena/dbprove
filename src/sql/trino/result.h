#pragma once

#include <vector>

#include "result_base.h"

namespace sql::trino {
class Row;

class Result final : public ResultBase {
  friend class Row;

  std::vector<std::vector<SqlVariant>> rows_;
  std::unique_ptr<Row> current_row_;
  ColumnCount column_count_ = 0;
  RowCount current_row_index_ = 0;

  const std::vector<SqlVariant>& currentRow() const;
  SqlVariant columnData(size_t index) const;

public:
  explicit Result(std::vector<std::vector<SqlVariant>> rows, ColumnCount column_count);
  ~Result() override;

  RowCount rowCount() const override;
  ColumnCount columnCount() const override;

protected:
  const RowBase& nextRow() override;
};
}
