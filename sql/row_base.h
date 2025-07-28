#pragma once
#include <cstddef>
#include <stdexcept>

#include "sql_type.h"
#include <variant>

namespace sql {
class RowBase {
protected:
  /// @brief Implement to return value of column
  [[nodiscard]] virtual SqlVariant get(size_t index) const = 0;

public:
  RowBase() = default;
  virtual ~RowBase() = default;

  /// @brief Returns the value of the column at the specified index.
  SqlVariant operator[](const size_t index) const {
    return get(index);
  }
  template<typename T>
  T asSqlType(const size_t index) const {
    const auto v = get(index);
    if (!v.is<T>()) {
      throw std::runtime_error("Invalid type access at index " + std::to_string(index));

    }
    return get(index).get<T>();
  }

  double asDouble(const size_t index) const {
    const auto v = get(index);
    if (!v.is<SqlDouble>()) {
      throw std::runtime_error("Not a double");
    }
    return v.get<SqlDouble>().get();
  }

};
}