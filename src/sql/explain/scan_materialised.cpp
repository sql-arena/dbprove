#include "scan_materialised.h"

namespace sql::explain {
std::string ScanMaterialised::compactSymbolic() const {
  std::string result = "SCAN MATERIALISED";
  if (primary_node_id >= 0) {
    result += " NODE " + std::to_string(primary_node_id);
  }
  if (!expression.empty()) {
    result += " (" + expression + ")";
  }
  return result;
}

std::string ScanMaterialised::renderMuggle(size_t max_width) const {
  std::string result = "SCAN MATERIALISED";
  if (primary_node_id >= 0) {
    result += " NODE " + std::to_string(primary_node_id);
  }
  if (!expression.empty()) {
    result += " (" + expression + ")";
  }
  return result;
}

std::string ScanMaterialised::treeSQLImpl(size_t indent) const {
  return "(SELECT 1 ) AS " + subquerySQLAlias();
}
}

