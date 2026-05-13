#pragma once

#include <dbprove/sql/result_base.h>

namespace sql::datafusion {
class Result final : public ResultBase {
  std::vector<std::vector<SqlVariant>> rows_;
  std::unique_ptr<RowBase> current_row_;
  ColumnCount column_count_ = 0;
  size_t current_row_index_ = 0;

public:
  Result(std::vector<std::vector<SqlVariant>> rows, std::vector<SqlTypeKind> column_types);
  ~Result() override;

  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  const std::vector<SqlVariant>& currentRow() const;
  SqlVariant columnData(size_t index) const;

protected:
  const RowBase& nextRow() override;
  void reset() override;
};
}
