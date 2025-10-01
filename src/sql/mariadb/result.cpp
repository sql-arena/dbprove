#include "result.h"

#include <map>

#include "row.h"
#include "sql_exceptions.h"
#include <duckdb/common/exception.hpp>
#include <mysql/mysql.h>


namespace sql::mariadb {
class Result::Pimpl {
public:
  MYSQL_RES* result_;
  const ColumnCount columnCount_;
  const RowCount rowCount_;
  std::unique_ptr<Row> currentRow_;
  MYSQL_ROW currentRowData;

  explicit Pimpl(void* handle, Result* result)
    : result_(static_cast<MYSQL_RES*>(handle))
    , columnCount_(mysql_num_fields(result_))
    , rowCount_(mysql_num_rows(result_))
    , currentRow_(std::make_unique<Row>(result)) {
  }

  ~Pimpl() {
    mysql_free_result(result_);
  };
};

const char* Result::columnData(const size_t index) const {
  return impl_->currentRowData[index];
}

Result::Result(void* mysql_result)
  : impl_(std::make_unique<Pimpl>(mysql_result, this)) {
  const static std::map<enum_field_types, SqlTypeKind> type_map = {
      {MYSQL_TYPE_SHORT, SqlTypeKind::SMALLINT},
      {MYSQL_TYPE_LONG, SqlTypeKind::INT},
      {MYSQL_TYPE_LONGLONG, SqlTypeKind::BIGINT},
      {MYSQL_TYPE_FLOAT, SqlTypeKind::DOUBLE},
      {MYSQL_TYPE_DOUBLE, SqlTypeKind::DOUBLE},
      {MYSQL_TYPE_STRING, SqlTypeKind::STRING},
      {MYSQL_TYPE_VAR_STRING, SqlTypeKind::STRING},
      {MYSQL_TYPE_DECIMAL, SqlTypeKind::DECIMAL}
  };

  const MYSQL_FIELD* fields = mysql_fetch_fields(impl_->result_);
  for (size_t i = 0; i < columnCount(); ++i) {
    auto mysql_type = fields[i].type;
    if (!type_map.contains(mysql_type)) {
      throw InvalidTypeException("Unsupported type " + std::to_string(mysql_type));
    }
    columnTypes_.push_back(type_map.at(mysql_type));
  }
}

RowCount Result::rowCount() const {
  return impl_->rowCount_;
}

ColumnCount Result::columnCount() const {
  return impl_->columnCount_;
}

Result::~Result() {
}

const RowBase& Result::nextRow() {
  if (currentRowIndex_ >= rowCount()) {
    return SentinelRow::instance();
  }
  impl_->currentRowData = mysql_fetch_row(impl_->result_);
  ++currentRowIndex_;
  return *impl_->currentRow_;
}
}