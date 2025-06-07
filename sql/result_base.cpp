#include "row_iterator.h"
#include "result_base.h"

namespace sql {
// Implement rows() method for IResult
RowIterable ResultBase::rows() {
  reset(); // Reset the cursor position before creating iterator
  return RowIterable(*this);
}
}