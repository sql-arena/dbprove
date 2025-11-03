#include "selection.h"

namespace sql::explain {
Selection::Selection(const std::string& filter_expression)
  : Node(NodeType::FILTER) {
  setFilter(filter_expression);
}
}