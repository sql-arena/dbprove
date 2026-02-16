#include "result.h"
#include "row.h"
namespace sql::snowflake {


  class Result::Pimpl {
    void* handle_;
    public:
      Pimpl(void* handle): handle_(handle) {}
  };

  Result::Result(void* handle)
    : impl_(std::make_unique<Pimpl>(handle)) {
  }

  Result::~Result()
  {
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