#include "result.h"
#include "row.h"
#include "sql_exceptions.h"
#include "block_holder.h"
#include <clickhouse/client.h>
#include <utility>

namespace ch = clickhouse;

namespace sql::clickhouse {
class Result::Pimpl {
public:
  std::unique_ptr<BlockHolder> h;
  std::unique_ptr<Row> row;
  ColumnCount columnCount;
  RowCount rowCount = 0;
  size_t currentBlockOffset = 0;
  size_t currentRowOffset = 0;

  ch::Block& currentBlock() const {
    return *h->blocks[currentBlockOffset].get();
  }

  // Add other members as needed
  explicit Pimpl(std::unique_ptr<BlockHolder> b, Result* parent)
    : h(std::move(std::move(b)))
    , columnCount(h->blocks.front()->GetColumnCount())
    , row(std::make_unique<Row>(parent)
        ) {
    for (const auto block : h->blocks) {
      rowCount += block->GetRowCount();
    }
    // THe very first row is a meta row
    while (currentBlock().GetRowCount() == 0) {
      currentBlockOffset++;
    }
  }

  ~Pimpl() = default;
};

Result::Result(std::unique_ptr<BlockHolder> holder)
  : impl_(std::make_unique<Pimpl>(std::move(holder), this)) {
}

RowCount Result::rowCount() const {
  return impl_->rowCount;
}

ColumnCount Result::columnCount() const {
  return impl_->columnCount;
}

Result::~Result() = default;

const RowBase& Result::nextRow() {
  if (currentRowIndex_ >= rowCount()) {
    return SentinelRow::instance();
  }
  currentRowIndex_++;
  if (impl_->currentRowOffset >= impl_->currentBlock().GetRowCount()) {
    impl_->currentBlockOffset++;
    impl_->currentRowOffset = 0;
  } else {
    impl_->currentRowOffset++;
  }
  return *impl_->row;
}

std::string to_string(ch::Int128& v, size_t digits) {
  std::string result;
  while (digits--) {
    const auto digit = static_cast<uint8_t>(v % 10);
    result += std::to_string(digit);
    v /= 10;
  }
  std::ranges::reverse(result);
  return result;
}

std::string DecimalToString(const ch::ColumnDecimal& col, size_t index) {
  using Int128 = ch::Int128;
  const Int128 value = col.At(index);
  const size_t scale = col.GetScale();
  const size_t precision = col.GetPrecision();
  const bool negative = value < 0;
  Int128 abs_value = negative ? -value : value;

  std::string result;
  result.reserve(precision + 2); // Sign and period
  if (negative) {
    result += "-";
  }
  if (precision == scale) {
    return "0." + to_string(abs_value, scale);
  }
  result += to_string(abs_value, precision - scale);
  if (precision > scale) {
    result += ".";
  }
  result += to_string(abs_value, scale);
  return result;
}

SqlVariant Result::getRowValue(size_t index) const {
  const auto& b = impl_->currentBlock();
  const auto offset = impl_->currentRowOffset - 1;
  auto& column = *b[index];
  switch (column.GetType().GetCode()) {
    case ::clickhouse::Type::Int8:
      return SqlVariant(column.As<::clickhouse::ColumnInt8>()->At(offset));
    case ::clickhouse::Type::Int16:
      return SqlVariant(column.As<::clickhouse::ColumnInt16>()->At(offset));
    case ch::Type::Code::Int32:
      return SqlVariant(column.As<::clickhouse::ColumnInt32>()->At(offset));
    case ::clickhouse::Type::Int64:
      return SqlVariant(column.As<::clickhouse::ColumnInt64>()->At(offset));
    case ::clickhouse::Type::UInt64:
      // Clickhouse returns COUNT(*) as this type
      return SqlVariant(static_cast<int64_t>(column.As<::clickhouse::ColumnUInt64>()->At(offset)));
    case ::clickhouse::Type::Float32:
      return SqlVariant(column.As<ch::ColumnFloat32>()->At(offset));
    case ::clickhouse::Type::Float64:
      return SqlVariant(column.As<ch::ColumnFloat64>()->At(offset));
    case ::clickhouse::Type::String:
    case ::clickhouse::Type::FixedString:
      return SqlVariant(column.As<ch::ColumnString>()->At(offset));
    case ::clickhouse::Type::Decimal:
    case ::clickhouse::Type::Decimal32:
    case ::clickhouse::Type::Decimal64:
    case ::clickhouse::Type::Decimal128: {
      return SqlVariant(DecimalToString(*column.As<ch::ColumnDecimal>(), offset));
    }
    case ::clickhouse::Type::Date32:
    case ::clickhouse::Type::DateTime:
    case ::clickhouse::Type::DateTime64:
    case ::clickhouse::Type::Date:
    default:
      throw InvalidTypeException("Unsupported ClickHouse Type: " + column.GetType().GetName());
  }
}
} // namespace sql::clickhouse