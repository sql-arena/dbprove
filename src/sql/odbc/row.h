#pragma once
#include "result.h"
#include "dbprove/sql/row_base.h"
#include <dbprove/sql/sql_type.h>


namespace sql::odbc {
class Result;

class Row final : public RowBase {
  Result& result_;

protected:
  [[nodiscard]] SqlVariant get(size_t index) const override;

public:
  explicit Row(Result& result)
    : result_(result) {
  }

  ColumnCount columnCount() const override;
};
}