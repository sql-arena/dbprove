#pragma once
#include <utility>

#include "sql_type.h"

namespace sql::explain {
class Column {
public:
  explicit Column(SqlVariant sql_type)
    : sql_type(std::move(sql_type)) {
  }

  SqlVariant sql_type;

  enum class Sorting {
    ASC, DESC, RANDOM
  };

  enum class Distribution {
    HASH, RANDOM, REPLICATED
  };

  Sorting sorting = Sorting::RANDOM;
  Distribution distribution = Distribution::RANDOM;
};
} // namespace sql::explain