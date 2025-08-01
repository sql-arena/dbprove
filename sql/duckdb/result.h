#pragma once
#include <result_base.h>
#include <cstddef>
#include <atomic>
#include "row_base.h"
#include "row.h"

namespace duckdb
{
  class QueryResult;
  class DataChunk;
}

namespace sql::duckdb {


class Result final : public ResultBase {
  const ColumnCount columnCount_;
  mutable std::unique_ptr<::duckdb::QueryResult> result_;
  std::unique_ptr<::duckdb::DataChunk> currentChunk_;
  size_t currentChunkIndex_;
  Row currentRow_;

  std::atomic<int> rowNumber_;
public:
  explicit Result(std::unique_ptr<::duckdb::QueryResult> duckResult);;

  ~Result() override {
  };

  size_t rowCount() const override;;
  size_t columnCount() const override { return columnCount_; };


protected:
  void reset() override {
  };

  virtual const RowBase& nextRow();;


};

}
