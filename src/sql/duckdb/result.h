#pragma once
#include "row.h"
#include <dbprove/sql/sql.h>
#include <duckdb/main/query_result.hpp>
#include <cstddef>
#include <atomic>


namespace sql::duckdb {
class Connection;

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

  friend class Connection;
protected:
  void reset() override {
  };

  const RowBase& nextRow() override;
};

}
