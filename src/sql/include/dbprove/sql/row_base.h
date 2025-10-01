#pragma once
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#include "sql_type.h"

namespace sql {
class MaterialisedRow;

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

  template <>
  SqlInt asSqlType(const size_t index) const {
    const auto v = get(index);
    if (v.is<SqlSmallInt>()) {
      return SqlInt(v.get<SqlSmallInt>().get());
    }
    if (v.is<SqlInt>()) {
      return v.get<SqlInt>();
    }
    if (v.is<SqlBigInt>()) {
      const auto i8 = v.get<SqlBigInt>().get();
      if (i8 < std::numeric_limits<int32_t>::min()) {
        throw std::runtime_error("Value is too small  to fit in an int32");
      }
      if (i8 > std::numeric_limits<int32_t>::max()) {
        throw std::runtime_error("Value is too large  to fit in an int32");
      }
      return SqlInt(static_cast<int32_t>(i8));
    }

    throw std::runtime_error("Attempting to access non integer as SqlInt at index " + std::to_string(index));
  }

  [[nodiscard]] SqlVariant asVariant(const size_t index) const {
    return get(index);
  }

  [[nodiscard]] std::string asString(const size_t index) const {
    const auto v = get(index);
    if (!v.is<SqlString>()) {
      throw std::runtime_error("Not a string");
    }
    return v.get<SqlString>().get();
  }

  [[nodiscard]] double asDouble(const size_t index) const {
    const auto v = get(index);
    if (!v.is<SqlDouble>()) {
      throw std::runtime_error("Not a double");
    }
    return v.get<SqlDouble>().get();
  }


  std::unique_ptr<MaterialisedRow> materialise() const;

  bool isSentinel() const;

  virtual bool operator==(const RowBase& other) const {
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
  ColumnCount columnCount() const override { return 0; };

protected:;
};


inline bool RowBase::isSentinel() const {
  return this == &SentinelRow::instance();
}

/**
 * Rows can turn into this data structure by calling Row::materialise()
 */
class MaterialisedRow final : public RowBase {
public:
  explicit MaterialisedRow(std::vector<SqlVariant> data)
    : data_(std::move(data)) {
  }

  ~MaterialisedRow() override = default;

  [[nodiscard]] ColumnCount columnCount() const override {
    return data_.size();
  };

protected:
  [[nodiscard]] SqlVariant get(size_t index) const override {
    return data_[index];
  };

private:
  std::vector<SqlVariant> data_;
};
}