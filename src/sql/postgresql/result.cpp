#include "result.h"
#include "row.h"
#include "parsers.h"
#include <libpq-fe.h>

#include "sql_exceptions.h"

namespace sql::postgresql {
class Result::Pimpl {
public:
  const int columnCount_;
  const int rowCount_;
  Row currentRow_;
  PGresult* data_;
  RowCount nextRowNumber;

  explicit Pimpl(PGresult* data, Result* result)
    : columnCount_(PQnfields(data))
    , rowCount_(PQntuples(data))
    , currentRow_(*result)
    , data_(data)
    , nextRowNumber(0) {
  }

  const RowBase& nextRow() {
    if (nextRowNumber >= rowCount_) {
      return SentinelRow::instance();
    }
    nextRowNumber++;
    return currentRow_;
  }
};

SqlVariant Result::get(size_t index) const {
  auto result = impl_->data_;
  auto row_number_ = impl_->nextRowNumber - 1;
  const int pg_field_num = static_cast<int>(index); // To match libpq internal representation
  const Oid pg_type = PQftype(result, pg_field_num);
  const char* binary_value = PQgetvalue(result, row_number_, pg_field_num);
  const int value_length = PQgetlength(result, row_number_, pg_field_num);

  switch (pg_type) {
    case INT2OID:
      return parseInt<int16_t>(binary_value);
    case INT4OID:
      return parseInt<int32_t>(binary_value);
    case INT8OID:
      return parseInt<int64_t>(binary_value);
    case FLOAT4OID:
      return parseFloat<float>(binary_value);
    case FLOAT8OID:
      return parseFloat<double>(binary_value);
    case VARCHAROID:
    case TEXTOID:
    case JSONOID:
      return SqlVariant(std::string(PQgetvalue(result, row_number_, pg_field_num)));
    case NUMERICOID:
      return parseDecimal(binary_value, value_length);
  }
  throw InvalidTypeException("The OID '" + std::to_string(pg_type) + "' cannot be mapped to a type");
}

Result::Result(void* data)
  : impl_(std::make_unique<Pimpl>(static_cast<PGresult*>(data), this)) {
}

Result::~Result() {
}

RowCount Result::rowCount() const { return impl_->rowCount_; }

ColumnCount Result::columnCount() const { return impl_->columnCount_; }

const RowBase& Result::nextRow() {
  return impl_->nextRow();
}
}
