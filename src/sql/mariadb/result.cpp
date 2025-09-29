#include "result.h"
#include "row.h"

namespace sql::mariadb {
class Result::Pimpl {
  void* handle_;

public:
  explicit Pimpl(void* handle)
    : handle_(handle) {
  }
};

Result::Result(void* handle)
  : impl_(std::make_unique<Pimpl>(handle)) {
}

RowCount Result::rowCount() const {
  // TODO: Implement
  return 0;
}

ColumnCount Result::columnCount() const {
  // TODO: Implement
  return 0;
}

const RowBase& Result::nextRow() {
  if (currentRowIndex_ > rowCount()) {
    return SentinelRow::instance();
  }

  // TODO: Implement, you likely want to maintain a reference to the data here
  return *new Row();
}
}