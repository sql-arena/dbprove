#include "scan_materialised.h"
#include "materialise.h"

namespace sql::explain {
void ScanMaterialised::setSourceMaterialise(const Materialise* source) {
  source_materialise = source;
}

const Materialise* ScanMaterialised::sourceMaterialise() const {
  return source_materialise;
}

std::string ScanMaterialised::materialisedAlias() const {
  if (!primary_node_name.empty()) {
    return "m_" + primary_node_name;
  }
  if (primary_node_id >= 0) {
    return "m" + std::to_string(primary_node_id);
  }
  return "m" + std::to_string(id());
}

std::string ScanMaterialised::compactSymbolic() const {
  std::string result = "SCAN MATERIALISED";
  if (!primary_node_name.empty()) {
    result += " " + primary_node_name;
  } else if (primary_node_id >= 0) {
    result += " " + materialisedAlias();
  }
  return result;
}

std::string ScanMaterialised::renderMuggle(size_t max_width) const {
  std::string result = "SCAN MATERIALISED";
  if (!primary_node_name.empty()) {
    result += " " + primary_node_name;
  } else if (primary_node_id >= 0) {
    result += " " + materialisedAlias();
  }
  return result;
}

std::string ScanMaterialised::treeSQLImpl(size_t indent) const {
  if (source_materialise != nullptr && source_materialise->childCount() > 0) {
    const auto* source_child = source_materialise->firstChild();
    if (source_child != nullptr && source_child != this) {
      auto* mutable_source = const_cast<Materialise*>(source_materialise);
      return mutable_source->treeSQL(indent);
    }
  }
  if (!expression.empty()) {
    return "(SELECT " + expression + " ) AS " + materialisedAlias();
  }
  return "(SELECT 1 ) AS " + materialisedAlias();
}

std::string ScanMaterialised::actualsSql() {
  if (source_materialise != nullptr && source_materialise->childCount() > 0) {
    return "SELECT COUNT(*)\nFROM " + source_materialise->firstChild()->treeSQL(0);
  }
  return Node::actualsSql();
}
}
