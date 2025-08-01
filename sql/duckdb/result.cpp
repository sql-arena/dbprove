#include "result.h"
#include "../row_base.h"
#include <duckdb.hpp>


namespace sql::duckdb {
Result::Result(std::unique_ptr<::duckdb::QueryResult> duckResult)
  : columnCount_(static_cast<ColumnCount>(duckResult->ColumnCount())
      )
  , rowNumber_(0)
  , result_(std::move(duckResult)) {
}

size_t Result::rowCount() const {
  assert(result_->type != ::duckdb::QueryResultType::STREAM_RESULT);
  const auto materialized = static_cast<::duckdb::MaterializedQueryResult*>(result_.get());
  return materialized->RowCount();
}

const RowBase& Result::nextRow() {
  if (!currentChunk_ || currentChunkIndex_ >= currentChunk_->size()) {
    // Fetch the next chunk from the result
    currentChunk_ = result_->Fetch();
    currentChunkIndex_ = 0;

    // If no more chunks available, return an empty row or throw
    if (!currentChunk_ || currentChunk_->size() == 0) {
      throw std::runtime_error("No more rows available");
    }
  }

  currentRow_ = Row(currentChunk_.get(), currentChunkIndex_, rowNumber_++);
  return currentRow_;
}
}