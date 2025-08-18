#include "scan_empty.h"

static constexpr const char* symbol_ = "📄";

namespace sql::explain {
std::string ScanEmpty::compactSymbolic() const {
  return std::string(symbol_);
}

std::string ScanEmpty::renderMuggle(size_t max_width) const {
  return "SCAN EMPTY";
}
}