#include "explain/column.h"

#include "sql.h"

namespace sql::explain {
Column::Column(std::string name, const EngineDialect* dialect)
  : Column(std::move(name), Sorting::RANDOM, dialect) {
}

Column::Column(const std::string& name, const Sorting sorting, const EngineDialect* dialect)
  : name(sql::cleanExpression(name, dialect))
  , sorting(sorting) {
}

Column::Column(const std::string& name, const std::string& alias, const EngineDialect* dialect)
  : name(sql::cleanExpression(name, dialect))
  , alias(sql::cleanExpression(alias, dialect)) {
}
}