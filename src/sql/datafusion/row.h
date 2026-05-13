#pragma once

#include <dbprove/sql/row_base.h>

namespace sql::datafusion {
class Result;

class Row final : public RowBase {
  const Result* result_;

public:
  explicit Row(const Result* result)
    : result_(result) {
  }

  ColumnCount columnCount() const override;

protected:
  SqlVariant get(size_t index) const override;
};
}
