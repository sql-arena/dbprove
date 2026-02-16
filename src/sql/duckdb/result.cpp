#include "result.h"
#include "result_holder.h"
#include "parsers.h"
#include <dbprove/sql/row_base.h>
#include <duckdb.hpp>

#include "sql_exceptions.h"


namespace sql::duckdb {
class Result::Pimpl {
public:
  Result& result;
  mutable std::unique_ptr<::duckdb::QueryResult> duck_result_;
  const ColumnCount columnCount_;
  std::unique_ptr<::duckdb::DataChunk> currentChunk_;
  size_t nextChunkIndex;
  Row currentRow_;
  RowCount rowNumber_;

  explicit Pimpl(ResultHolder& duck_result, Result& result)
    : result(result)
    , duck_result_(std::move(duck_result.result))
    , columnCount_(duck_result_->ColumnCount())
    , nextChunkIndex()
    , currentRow_(result)
    , rowNumber_(0)
    , currentChunk_(nullptr) {
  }

  const RowBase& nextRow() {
    if (!currentChunk_) {
      // Next chunk from the result
      currentChunk_ = duck_result_->Fetch();
      nextChunkIndex = 0;
    }
    if (!currentChunk_) {
      // This was the final chunk.
      return SentinelRow::instance();
    }
    if (currentChunk_->size() == 0) {
      // Empty result
      return SentinelRow::instance();
    }
    // Inside a chunk
    if (nextChunkIndex >= currentChunk_->size()) {
      // End of current chunk
      currentChunk_ = nullptr;
      return nextRow();
    }
    ++nextChunkIndex;
    return currentRow_;
  }
};

RowCount Result::rowNumber() const {
  return impl_->rowNumber_;
}

SqlVariant Result::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Column index out of range");
  }

  const auto& column = impl_->currentChunk_->data[index];
  const auto current_chunk_index = impl_->nextChunkIndex - 1;
  switch (column.GetType().id()) {
    case ::duckdb::LogicalTypeId::BOOLEAN:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<bool>());

    case ::duckdb::LogicalTypeId::TINYINT:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<int16_t>());

    case ::duckdb::LogicalTypeId::SMALLINT:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<int16_t>());

    case ::duckdb::LogicalTypeId::INTEGER:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<int32_t>());

    case ::duckdb::LogicalTypeId::BIGINT:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<int64_t>());

    case ::duckdb::LogicalTypeId::FLOAT:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<float>());

    case ::duckdb::LogicalTypeId::DOUBLE:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<double>());

    case ::duckdb::LogicalTypeId::VARCHAR:
      return SqlVariant(column.GetValue(current_chunk_index).GetValue<std::string>());

    case ::duckdb::LogicalTypeId::DECIMAL: {
      // Get width and scale information from the type
      auto& type = column.GetType();
      const int width = ::duckdb::DecimalType::GetWidth(type);
      const int scale = ::duckdb::DecimalType::GetScale(type);
      if (width <= 4) {
        // 1-4 digits stored as int16
        const auto value = column.GetValue(current_chunk_index).GetValue<int16_t>();
        return parseDecimal(value, scale);
      }
      if (width <= 9) {
        // 5-9 digits stored as int32
        const auto value = column.GetValue(current_chunk_index).GetValue<int32_t>();
        return parseDecimal(value, scale);
      }
      if (width <= 18) {
        // 10-18 digits stored as int64
        const auto value = column.GetValue(current_chunk_index).GetValue<int64_t>();
        return parseDecimal(value, scale);
      }
      // > 18 digits is uint128
      const auto value = column.GetValue(current_chunk_index).GetValue<::duckdb::hugeint_t>();
      return parseDecimal(value, scale);
    }
    case ::duckdb::LogicalTypeId::SQLNULL:
      return SqlVariant();
    default:
      throw InvalidTypeException("Unsupported DuckDb column type: " + column.GetType().ToString());
  }
}

Result::Result(ResultHolder& duck_result)
  : impl_(std::make_unique<Pimpl>(duck_result, *this)) {
}

Result::~Result() {
}

RowCount Result::rowCount() const {
  assert(impl_->duck_result_->type != ::duckdb::QueryResultType::STREAM_RESULT);
  const auto materialized = static_cast<::duckdb::MaterializedQueryResult*>(impl_->duck_result_.get());
  return materialized->RowCount();
}

ColumnCount Result::columnCount() const { return impl_->columnCount_; }

const RowBase& Result::nextRow() {
  return impl_->nextRow();
}
}