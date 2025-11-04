#include "scan.h"
#include "glyphs.h"

static constexpr auto symbol_ = "📄";


namespace sql::explain {
std::string to_string(Scan::Strategy strategy) {
  return std::string(magic_enum::enum_name(strategy));
}

Scan::Scan(const std::string& table_name, Strategy strategy)
  : Node(NodeType::SCAN)
  , strategy(strategy)
  , table_name(cleanExpression(table_name))
  , schema_name(splitTable(table_name).schema_name) {
}

std::string Scan::compactSymbolic() const {
  return table_name;
}

std::string Scan::renderMuggle(size_t max_width) const {
  std::string result = "TABLE " + to_string(strategy) + " " + table_name;
  if (!filter_condition.empty()) {
    result += " WHERE ";
    max_width -= result.size();
    result += ellipsify(filter_condition, max_width);
  }
  return result;
}

std::string Scan::treeSQLImpl(size_t indent) const {
  std::string result = "(SELECT * FROM " + schema_name + "." + table_name + " ";
  if (!filterCondition().empty()) {
    result += "WHERE " + filterCondition();
  }
  result += ") AS scan_" + nodeName();
  return result;
}
}