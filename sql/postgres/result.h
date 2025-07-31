#pragma once
#include <result_base.h>
#include <cstddef>
#include <atomic>
#include "row_base.h"
#include "libpq-fe.h"
#include "row.h"

namespace sql::postgres {


class Result : public ResultBase {
  const int columnCount_;
  const int rowCount_;
  Row currentRow_;
  PGresult* data_;
  std::atomic<int> rowNumber_;
public:
  explicit Result(PGresult* data)
    : columnCount_(PQnfields(data)), rowCount_(PQntuples(data)), currentRow_(data), data_(data), rowNumber_(0) {
  };

  ~Result() override {
  };

  size_t rowCount() const override { return rowCount_; };
  size_t columnCount() const override { return columnCount_; };


protected:
  void reset() override {
  };

  virtual const RowBase& nextRow() ;


};

}
