#include "explain/column.h"

#include "sql.h"

namespace sql::explain {
Column::Column(std::string name)
  : Column(std::move(name), Sorting::RANDOM) {
}

Column::Column(const std::string& name, const Sorting sorting)
  : name(sql::cleanExpression(name))
  , sorting(sorting) {
}

Column::Column(const std::string& name, const std::string& alias)
  : name(name)
  , alias(alias) {
}
}