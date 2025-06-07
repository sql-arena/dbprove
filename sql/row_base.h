#pragma once
#include <cstddef>
#include "sql_type.h"

namespace sql {
class RowBase {
public:
  RowBase() = default;
  virtual ~RowBase() = default;

  /// @brief Returns the value of the column at the specified index.
  SqlVariant operator[](size_t index) const {
    return get(index);
  }

protected:
  /// @brief Implement to return value of column
  [[nodiscard]] virtual SqlVariant get(size_t index) const = 0;
};
}