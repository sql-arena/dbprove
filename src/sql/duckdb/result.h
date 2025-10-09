#pragma once
#include "row.h"
#include <dbprove/sql/sql.h>
#include <duckdb/main/query_result.hpp>


namespace sql::duckdb {
class Connection;
class ResultHolder;

class Result final : public ResultBase {
  class Pimpl;
  std::unique_ptr<Pimpl> impl_;
  friend class Row;
  RowCount rowNumber() const;
  SqlVariant get(size_t index) const;

public:
  explicit Result(ResultHolder& duck_result);

  ~Result() override;;

  size_t rowCount() const override;;
  size_t columnCount() const override;;

protected:
  void reset() override {
  };

  const RowBase& nextRow() override;
};
}
