#include "selection.h"

namespace sql::explain {
Selection::Selection(const std::string& filter_expression)
  : Node(NodeType::SELECTION)
  , filter_expression(cleanExpression(filter_expression)) {
}
}