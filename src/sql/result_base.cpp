#include "row_iterator.h"
#include "include/dbprove/sql/result_base.h"

namespace sql {
RowIterable ResultBase::rows() {
  reset(); // Reset the cursor position before creating iterator
  return RowIterable(*this);
}
}