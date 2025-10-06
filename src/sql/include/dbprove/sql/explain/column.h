#pragma once
#include <magic_enum/magic_enum.hpp>


namespace sql::explain {
class Column {
public:
  enum class Sorting {
    ASC, DESC, RANDOM
  };

  enum class Distribution {
    HASH, RANDOM, REPLICATED
  };

  explicit Column(std::string name);

  explicit Column(const std::string& name, const Sorting sorting);

  ~Column() = default;
  Column(const Column&) = default;

  bool operator==(const Column& other) const {
    return name == other.name;
  }

  const std::string name;
  const Sorting sorting = Sorting::RANDOM;
  const Distribution distribution = Distribution::RANDOM;
};

inline std::string_view to_string(const Column::Sorting sorting) {
  return magic_enum::enum_name(sorting);
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