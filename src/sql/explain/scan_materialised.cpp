#include "scan_materialised.h"

namespace sql::explain {
std::string ScanMaterialised::compactSymbolic() const {
  return "SCAN MATERIALISED";
}

std::string ScanMaterialised::renderMuggle(size_t max_width) const {
  return "SCAN MATERIALISED";
}
}

