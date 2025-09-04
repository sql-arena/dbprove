#pragma once
#include "result.h"
#include "row_base.h"


namespace sql::clickhouse {
class Row final : public RowBase {
protected:
  [[nodiscard]] SqlVariant get(size_t index) const override;

public:
  [[nodiscard]] ColumnCount columnCount() const override;
  explicit Row(Result* result);

private:
  friend class Result;
  Result* result_;
};
}
