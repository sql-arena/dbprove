#include "result.h"
#include "row.h"

namespace sql::yellowbrick {
Result::Result(PGresult* data)
  : postgres::Result(data) {
}
}