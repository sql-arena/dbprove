#include "row.h"
#include "result.h"
#include "sql_exceptions.h"
#include <dbprove/sql/sql.h>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/hugeint.hpp>


namespace sql::duckdb {
Row::Row(const Row& row, Result* owningResult)
  : dataChunk_(row.dataChunk_)
  , ownsResult_(owningResult) {
  assert(row.dataChunk_);
}


Row::~Row() {
  delete ownsResult_;
}

ColumnCount Row::columnCount() const {
  return dataChunk_->ColumnCount();
}

template <typename T>
SqlVariant parseDecimal(T value, uint16_t scale) {
  // We may need to "shift" the value up, so operate on absolute values first
  std::string str = std::to_string(std::abs(value));

  // Pad with leading zeros if needed
  if (str.length() <= scale) {
    const std::string zeros(scale - str.length() + 1, '0');
    str = zeros + str;
  }

  // Insert decimal point
  if (scale > 0) {
    str.insert(str.length() - scale, ".");
  }

  // Add negative sign if needed
  if (value < 0) {
    str = "-" + str;
  }

  return SqlVariant(SqlDecimal(str));
}

template <>
SqlVariant parseDecimal(::duckdb::hugeint_t value, uint16_t scale) {
  // Get string representation with DuckDB's Hugeint::ToString
  std::string str;

  // Check if the value is negative
  bool isNegative = value < 0;

  // Use DuckDB's own ToString method which handles hugeint_t correctly
  str = ::duckdb::Hugeint::ToString(isNegative ? -value : value);

  // Pad with leading zeros if needed
  if (str.length() <= scale) {
    const std::string zeros(scale - str.length() + 1, '0');
    str = zeros + str;
  }

  // Insert decimal point
  if (scale > 0) {
    str.insert(str.length() - scale, ".");
  }

  // Add negative sign if needed
  if (isNegative) {
    str = "-" + str;
  }

  return SqlVariant(SqlDecimal(str));
}


SqlVariant Row::get(const size_t index) const {
  if (index >= columnCount()) {
    throw InvalidColumnsException("Column index out of range");
  }
  const auto& column = dataChunk_->data[index];

  switch (column.GetType().id()) {
    case ::duckdb::LogicalTypeId::BOOLEAN:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<bool>());

    case ::duckdb::LogicalTypeId::TINYINT:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<int8_t>());

    case ::duckdb::LogicalTypeId::SMALLINT:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<int16_t>());

    case ::duckdb::LogicalTypeId::INTEGER:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<int32_t>());

    case ::duckdb::LogicalTypeId::BIGINT:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<int64_t>());

    case ::duckdb::LogicalTypeId::FLOAT:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<float>());

    case ::duckdb::LogicalTypeId::DOUBLE:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<double>());

    case ::duckdb::LogicalTypeId::VARCHAR:
      return SqlVariant(column.GetValue(chunkIndex_).GetValue<std::string>());

    case ::duckdb::LogicalTypeId::DECIMAL: {
      // Get width and scale information from the type
      auto& type = column.GetType();
      const int width = ::duckdb::DecimalType::GetWidth(type);
      const int scale = ::duckdb::DecimalType::GetScale(type);
      if (width <= 4) {
        // 1-4 digits stored as int16
        const auto value = column.GetValue(chunkIndex_).GetValue<int16_t>();
        return parseDecimal(value, scale);
      }
      if (width <= 9) {
        // 5-9 digits stored as int32
        const auto value = column.GetValue(chunkIndex_).GetValue<int32_t>();
        return parseDecimal(value, scale);
      }
      if (width <= 18) {
        // 10-18 digits stored as int64
        const auto value = column.GetValue(chunkIndex_).GetValue<int64_t>();
        return parseDecimal(value, scale);
      }
      // > 18 digits is uint128
      const auto value = column.GetValue(chunkIndex_).GetValue<::duckdb::hugeint_t>();
      return parseDecimal(value, scale);
    }
    case ::duckdb::LogicalTypeId::SQLNULL:
      return SqlVariant();
    default:
      throw InvalidTypeException("Unsupported column type: " + column.GetType().ToString());
  }
}
}