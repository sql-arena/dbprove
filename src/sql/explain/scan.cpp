#include "scan.h"
#include "glyphs.h"

static constexpr auto symbol_ = "📄";


namespace sql::explain {
std::string to_string(Scan::Strategy strategy) {
  return std::string(magic_enum::enum_name(strategy));
}

Scan::Scan(const std::string& table_name, Strategy strategy, const std::string& alias, const EngineDialect* dialect)
  : Node(NodeType::SCAN)
  , strategy(strategy)
  , table_name(cleanExpression(table_name, dialect))
  , alias(cleanExpression(alias, dialect))
  , schema_name(splitTable(table_name).schema_name) {
}

std::string Scan::compactSymbolic() const {
  if (alias.empty()) {
    return table_name;
  }
  return table_name + " AS " + alias;
}

std::string Scan::renderMuggle(size_t max_width) const {
  std::string name = table_name;
  if (!alias.empty()) {
    name += " AS " + alias;
  }
  std::string result = "TABLE " + to_string(strategy) + " " + name;
  if (!filter_condition.empty()) {
    result += " WHERE ";
    max_width -= result.size();
    result += ellipsify(filter_condition, max_width);
  }
  return result;
}

std::string Scan::treeSQLImpl(size_t indent) const {
  std::string full_table_name = schema_name.empty() ? table_name : schema_name + "." + table_name;
  std::string result = "(SELECT * FROM " + full_table_name + " ";
  if (!alias.empty()) {
    result += "AS " + alias + " ";
  }
  if (!filterCondition().empty()) {
    result += "WHERE " + filterCondition();
  }
  result += ") AS " + subquerySQLAlias();
  return result;
}
}