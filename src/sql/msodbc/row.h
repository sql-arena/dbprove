#pragma once
#include "result.h"
#include "row_base.h"


namespace sql::msodbc {
class Row final : public RowBase {
protected:
  [[nodiscard]] SqlVariant get(size_t index) const override;
  Result* result_;
  void* handle_;

public:
  explicit Row(Result* result, void* handle)
    : result_(result)
    , handle_(handle) {
  }

  ColumnCount columnCount() const override;
};
}
