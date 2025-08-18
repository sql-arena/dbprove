#pragma once
#include <utility>

#include "sql_type.h"

namespace sql::explain
{
  class Column
  {
  public:
    enum class Sorting
    {
      ASC, DESC, RANDOM
    };

    enum class Distribution
    {
      HASH, RANDOM, REPLICATED
    };

    explicit Column(const std::string& name)
      : name(name)
    {
    }

    explicit Column(const std::string& name, const Sorting sorting)
      : name(name)
      , sorting(sorting)
    {
    }

    bool operator==(const Column& other) const
    {
      return name == other.name;
    }

    template <typename Collection>
    static std::string join(const Collection& columns,
                            const std::string& delimiter,
                            size_t max_width = std::numeric_limits<size_t>::max(),
                            const bool with_order = false)
    {
      const std::string ellipsis = "...";
      if (columns.empty()) {
        return "";
      }
      std::string result;
      auto first = columns[0].name;
      if (first.size() > max_width) {
        return ellipsis;
      }
      result.append(columns[0].name);
      auto remaining_width = max_width - result.size() - ellipsis.size() - delimiter.size();
      for (size_t i = 1; i < columns.size(); ++i) {
        std::string add_column;
        add_column.append(delimiter);
        add_column.append(columns[i].name);
        if (with_order) {
          add_column.append(" ").append(columns[i].sorting == Sorting::ASC ? "ASC" : "DESC");
        }
        if (add_column.size() > remaining_width) {
          result.append(delimiter);
          result.append(ellipsis);
          break;
        }
        result.append(add_column);
        remaining_width -= add_column.size();
      }
      return result;
    }


    const std::string name;
    const Sorting sorting = Sorting::RANDOM;
    const Distribution distribution = Distribution::RANDOM;
  };
} // namespace sql::explain

namespace std
{
  template <>
  struct hash<sql::explain::Column>
  {
    size_t operator()(const sql::explain::Column& column) const
    {
      return hash<string>()(column.name);
    }
  };
}
