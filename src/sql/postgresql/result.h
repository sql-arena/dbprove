#pragma once
#include <result_base.h>
#include "row_base.h"

namespace sql::postgresql {
class Result : public ResultBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  friend class Row;
  SqlVariant get(size_t index) const;

public:
  explicit Result(void* data);

  ~Result() override;

  RowCount rowCount() const override;;
  ColumnCount columnCount() const override;;

protected:
  void reset() override {
  };

  const RowBase& nextRow() override;
};
}
