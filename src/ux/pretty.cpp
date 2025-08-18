#include <dbprove/ux/ux.h>
#include "glyphs.h"
#include <dbprove/common/pretty.h>

namespace dbprove::ux {
std::string PrettyRows(const size_t rows) {
  return common::PrettyHumanCount(rows);
}
}