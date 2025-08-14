#pragma once
#include <utility>

#include "sql_type.h"

namespace sql::explain {
class Column {
public:
  enum class Sorting {
    ASC, DESC, RANDOM
  };

  enum class Distribution {
    HASH, RANDOM, REPLICATED
  };

  explicit Column(const std::string& name)
    : name(name) {
  }

  explicit Column(const std::string& name, const Sorting sorting)
    : name(name)
    , sorting(sorting) {
  }

  bool operator==(const Column& other) const {
    return name == other.name;
  }

  template <typename Collection>
  static std::string join(const Collection& columns,
    const std::string& delimiter,
    const bool with_order = false) {
    if (columns.empty()) {
      return "";
    }
    std::string result;
    result.append(columns[0].name);
    for (size_t i = 1; i < columns.size(); ++i) {
      result.append(delimiter);
      result.append(columns[i].name);
      if (with_order) {
        result.append(" ").append(columns[i].sorting == Sorting::ASC ? "ASC" : "DESC");
      }
    }
    return result;
  }


  const std::string name;
  const Sorting sorting = Sorting::RANDOM;
  const Distribution distribution = Distribution::RANDOM;
};
} // namespace sql::explain

namespace std {
template <>
struct hash<sql::explain::Column> {
  size_t operator()(const sql::explain::Column& column) const {
    return hash<string>()(column.name);
  }
};
}