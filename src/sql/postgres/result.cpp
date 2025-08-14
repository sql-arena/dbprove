#include "result.h"
#include "../include/dbprove/sql/row_base.h"
namespace sql::postgres {
const RowBase& Result::nextRow()
{
  if (currentRow_.rowNumber() >= rowCount()) {
    return SentinelRow::instance();
  }
  currentRow_ = Row(data_, rowNumber_++);
  return currentRow_;
}
}
