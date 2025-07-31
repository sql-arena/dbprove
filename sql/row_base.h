#pragma once
#include <row_iterator.h>

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

  virtual ColumnCount columnCount() const = 0;
  template <typename T>
  T asSqlType(const size_t index) const {
    const auto v = get(index);
    if (!v.is<T>()) {
      throw std::runtime_error("Invalid type access at index " + std::to_string(index));
    }
    return get(index).get<T>();
  }

  SqlVariant asVariant(const size_t index) const {
    return get(index);
  }
  double asDouble(const size_t index) const {
    const auto v = get(index);
    if (!v.is<SqlDouble>()) {
      throw std::runtime_error("Not a double");
    }
    return v.get<SqlDouble>().get();
  }

  bool isSentinel() const;

  virtual bool operator==(const RowBase& other) const  {
    return false;
  };
};



class SentinelRow final : public RowBase {
public:
  bool operator==(const RowBase& other) const override {
    return this == &other;
  }
  static const RowBase& instance() {
    static SentinelRow emptyRow;
    return emptyRow;
  };
protected:
  [[nodiscard]] SqlVariant get(size_t index) const override { return SqlVariant(); }

public:
  ColumnCount columnCount() const override { return 0;};

protected:;
};

inline bool RowBase::isSentinel() const {
  return this == &SentinelRow::instance();
}

}