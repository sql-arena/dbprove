#pragma once
#include <vector>

#include "result_base.h"

namespace sql::msodbc {
class Row;

class Result final : public ResultBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  const std::vector<SqlVariant>& currentRow() const;
  RowCount currentRowIndex_ = 0;
  std::vector<SqlVariant> rowData_; ///< ODBC requires us to read columns in order. Materialize here.
  char bounceBuffer_[SqlType::MAX_STRING_LENGTH]; ///< Bounce buffer during parse
  void parseRow();
  SqlVariant odbc2SqlVariant(size_t index);

public:
  explicit Result(void* handle);
  ~Result() override;
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  friend class Row;

protected:
  const RowBase& nextRow() override;
};
}