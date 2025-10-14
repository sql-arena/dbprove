#pragma once
#include "row_base.h"

namespace sql::postgresql {
class Result;

class Row : public RowBase {
  Result& result_;

public:
  explicit Row(Result& result);

  ~Row() override;

  [[nodiscard]] ColumnCount columnCount() const override;;

protected:
  [[nodiscard]] SqlVariant get(size_t index) const override;
};
}
