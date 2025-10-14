#pragma once
#include <vector>

#include "result_base.h"

namespace sql::odbc {
class Row;

class Result final : public ResultBase {
  friend class Row;
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  SqlVariant get(size_t index) const;

public:
  explicit Result(void* handle);
  ~Result() override;
  RowCount rowCount() const override;
  ColumnCount columnCount() const override;
  friend class Row;
  ResultBase* nextResult() override;

protected:
  const RowBase& nextRow() override;
  void initResult(void* handle);
};
}