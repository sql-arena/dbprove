#include "selection.h"

namespace sql::explain {
Selection::Selection(const std::string& filter_expression, const EngineDialect* dialect)
  : Node(NodeType::FILTER) {
  setFilter(filter_expression, dialect);
}
}