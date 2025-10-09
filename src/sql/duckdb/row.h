#pragma once
#include <dbprove/sql/sql.h>


namespace duckdb {
class DataChunk;
}

namespace sql::duckdb {
class Result;

class Row final : public RowBase {
  Result& result_;

public:
  explicit Row(Result& result)
    : result_(result) {
  }

  ~Row() override;

  [[nodiscard]] ColumnCount columnCount() const override;;

  [[nodiscard]] RowCount rowNumber() const;

protected:
  [[nodiscard]] SqlVariant get(const size_t index) const override;
};
}