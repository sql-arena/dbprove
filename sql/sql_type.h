#pragma once
#include <cstdint>
#include <variant>
#include <string>

namespace sql {
using RowCount = uint64_t;
using ColumnCount = uint64_t;

class SqlType {
public:
  virtual ~SqlType() = default;
};

/// @brief SQL Types definition made so that we cannot mix up types by accident
template <typename T, typename Tag>
class SqlTypeDef final : SqlType {
  T value_;

public:
  constexpr SqlTypeDef() = default;

  constexpr explicit SqlTypeDef(T v)
    : value_(v) {
  }

  [[nodiscard]] constexpr T get() const { return value_; }

  template <typename U>
    requires std::is_convertible_v<T, U> && (sizeof(U) >= sizeof(T))
  constexpr operator U() const {
    return static_cast<U>(value_);
  }

  template <typename U>
    requires std::is_convertible_v<U, T>
  constexpr SqlTypeDef& operator=(U raw) {
    value_ = static_cast<T>(raw);
    return *this;
  }

  friend constexpr auto operator<=>(const SqlTypeDef&, const SqlTypeDef&) = default;
};


struct SqlTinyIntTag {
  static constexpr std::string_view name = "TINYINT";
};

struct SqlSmallIntTag {
  static constexpr std::string_view name = "SMALLINT";
};

struct SqlIntTag {
  static constexpr std::string_view name = "INT";
};

struct SqlBigIntTag {
  static constexpr std::string_view name = "BIGINT";
};

struct SqlTextTag {
  static constexpr std::string_view name = "TEXT";
};

struct SqlFloatTag {
  static constexpr std::string_view name = "FLOAT";
};

struct SqlDoubleTag {
  static constexpr std::string_view name = "DOUBLE";
};


using SqlTinyInt = SqlTypeDef<std::int8_t, SqlTinyIntTag>;
using SqlSmallInt = SqlTypeDef<std::int16_t, SqlSmallIntTag>;
using SqlInt = SqlTypeDef<std::int32_t, SqlIntTag>;
using SqlBigInt = SqlTypeDef<std::int64_t, SqlBigIntTag>;

using SqlFloat = SqlTypeDef<float, SqlFloatTag>;
using SqlDouble = SqlTypeDef<double, SqlDoubleTag>;

using SqlText = SqlTypeDef<std::string, SqlTextTag>;

class SqlVariant;

class SqlDecimal {
  using decimal = std::string;
  const decimal value_;

public:
  static constexpr std::string_view name = "DECIMAL";

  explicit SqlDecimal(const std::string_view value)
    : value_(value) {
  }

};


class SqlVariant {
  using variant_type = std::variant<SqlTinyInt,
                                    SqlSmallInt,
                                    SqlInt,
                                    SqlBigInt,
                                    SqlFloat,
                                    SqlDouble,
                                    SqlDecimal,
                                    SqlText>;
  variant_type data;

public:
  explicit SqlVariant(const variant_type& v)
    : data(v) {
  }

  explicit SqlVariant(const size_t v)
    : data(SqlBigInt(v)) {
  }

  explicit SqlVariant(const int64_t v)
    : data(SqlBigInt(v)) {
  }

  explicit SqlVariant(const int32_t v)
    : data(SqlInt(v)) {
  }

  explicit SqlVariant(const double v)
    : data(SqlDouble(v)) {
  }

  explicit SqlVariant(const std::string_view s)
    : data(SqlText(std::string(s))) {
  }

  explicit SqlVariant(const char* s)
  : data(SqlText(std::string(s))) {
  }

  explicit SqlVariant(std::string s)
    : data(SqlText(s)) {
  }


  /// @brief Is the value of the templated type?
  template <typename T>
  bool is() const {
    return std::holds_alternative<T>(data);
  }

  /// @brief Get the value of the templated type
  template <typename T>
  T get() const {
    return std::get<T>(data);
  }
};


}