#pragma once

#include "row_base.h"

namespace sql::trino {
class Result;

class Row final : public RowBase {
  Result* result_;

protected:
  SqlVariant get(size_t index) const override;

public:
  explicit Row(Result* result)
    : result_(result) {
  }

  ColumnCount columnCount() const override;
};
}
