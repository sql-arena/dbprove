#pragma once
#include <utility>
#include "glyphs.h"
#include <magic_enum/magic_enum.hpp>


namespace sql::explain {
class Column {
public:
  enum class Sorting {
    ASC, DESC, RANDOM
  };

  static std::string_view to_string(const Sorting sorting) {
    return magic_enum::enum_name(sorting);
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
                          size_t max_width = std::numeric_limits<size_t>::max(),
                          const bool with_order = false) {
    if (columns.empty()) {
      return "";
    }
    std::string result;
    auto remaining_width = max_width;
    size_t i = 0;
    do {
      std::string add_column;
      if (i > 1) {
        add_column.append(delimiter);
      }
      add_column.append(columns[i].name);
      if (with_order) {
        add_column.append(" ").append(to_string(columns[i].sorting));
      }
      result.append(ellipsify(add_column, remaining_width));
      remaining_width = max_width - result.size();
      i++;
    } while (i < columns.size() && remaining_width > kEllipsis.size() + delimiter.size());
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
