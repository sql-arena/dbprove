#include "result.h"

#include "row.h"


namespace sql::utopia {
RowCount Result::rowCount() const {
  switch (data) {
    case UtopiaData::EMPTY:
      return 0;
    case UtopiaData::N10:
      return 10;
    case UtopiaData::TEST_RESULT:
      return 3;
  default:
  }
  return 0;
}

ColumnCount Result::columnCount() const {
  switch (data) {
    case UtopiaData::EMPTY:
      return 1;
    case UtopiaData::N10:
      return 11;
    case UtopiaData::TEST_RESULT:
      return 2;
    default:
      return 0;
  }
  return 0;
}

const RowBase& utopia::Result::nextRow() {
  switch (data) {
    case UtopiaData::EMPTY: {
      return SentinelRow::instance();
    }
    case UtopiaData::N10: {
      if (rowNumber > rowCount()) {
        return SentinelRow::instance();
      }

      SqlVariant v(rowNumber);
      ++rowNumber;
      currentRow_ = Row(std::vector({v}));
      return currentRow_;
    }
    default: ;
  }
  return SentinelRow::instance();
}
}