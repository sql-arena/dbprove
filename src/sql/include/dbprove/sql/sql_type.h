#pragma once
#include <cstdint>
#include <utility>
#include <variant>
#include <string>
#include <stdexcept>

#include <magic_enum/magic_enum.hpp>

namespace sql {
using RowCount = uint64_t;
using ColumnCount = uint64_t;


inline void checkTableName(const std::string_view table) {
  for (const auto& c : table) {
    if (c >= 'A' && c <= 'Z') {
      throw std::runtime_error("Only lowercase table names are allowed");
    }
    if (std::isspace(c)) {
      throw std::runtime_error("No whitespace allowed in tables");
    }
    if (c == '\"' || c == '\'') {
      throw std::runtime_error("No quotes in table names");
    }
  }
}

enum class SqlTypeKind {
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  REAL,
  DOUBLE,
  DECIMAL,
  STRING,
  SQL_NULL
};

constexpr auto to_string(SqlTypeKind kind) {
  return magic_enum::enum_name(kind);
}

class SqlType {
public:
  static constexpr size_t MAX_STRING_LENGTH = 64 * 1024;
  virtual ~SqlType() = default;
};

/// @brief SQL Types definition made so that we cannot mix up types by accident
template <typename T, typename Tag>
class SqlTypeDef final : public SqlType {
  T value_;

public:
  constexpr SqlTypeDef() = default;

  constexpr explicit SqlTypeDef(T v)
    : value_(std::move(v)) {
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
  static constexpr auto kind = SqlTypeKind::TINYINT;
};

struct SqlSmallIntTag {
  static constexpr std::string_view name = "SMALLINT";
  static constexpr auto kind = SqlTypeKind::SMALLINT;
};

struct SqlIntTag {
  static constexpr std::string_view name = "INT";
  static constexpr auto kind = SqlTypeKind::INT;
};

struct SqlBigIntTag {
  static constexpr std::string_view name = "BIGINT";
  static constexpr auto kind = SqlTypeKind::BIGINT;
};

struct SqlStringTag {
  static constexpr std::string_view name = "STRING";
  static constexpr auto kind = SqlTypeKind::STRING;
};

struct SqlFloatTag {
  static constexpr std::string_view name = "FLOAT";
  static constexpr auto kind = SqlTypeKind::REAL;
};

struct SqlDoubleTag {
  static constexpr std::string_view name = "DOUBLE";
  static constexpr auto kind = SqlTypeKind::DOUBLE;
};


using SqlTinyInt = SqlTypeDef<std::int8_t, SqlTinyIntTag>;
using SqlSmallInt = SqlTypeDef<std::int16_t, SqlSmallIntTag>;
using SqlInt = SqlTypeDef<std::int32_t, SqlIntTag>;
using SqlBigInt = SqlTypeDef<std::int64_t, SqlBigIntTag>;

using SqlFloat = SqlTypeDef<float, SqlFloatTag>;
using SqlDouble = SqlTypeDef<double, SqlDoubleTag>;
using SqlString = SqlTypeDef<std::string, SqlStringTag>;

class SqlVariant;

class SqlDecimal {
  using decimal = std::string;
  decimal value_;

public:
  static constexpr std::string_view name = "DECIMAL";
  static constexpr auto kind = SqlTypeKind::DECIMAL;

  explicit SqlDecimal(const std::string_view value)
    : value_(value) {
  }

  SqlDecimal(const SqlDecimal& other)
    : value_(other.value_) {
  }

  SqlDecimal& operator=(const SqlDecimal& other) {
    if (this != &other) {
      value_ = other.value_;
    }
    return *this;
  }
};


class SqlNull {
public:
  static constexpr std::string_view name = "NULL";
  static constexpr auto kind = SqlTypeKind::SQL_NULL;
};

class SqlVariant {
  using variant_type = std::variant<SqlTinyInt,
                                    SqlSmallInt,
                                    SqlInt,
                                    SqlBigInt,
                                    SqlFloat,
                                    SqlDouble,
                                    SqlDecimal,
                                    SqlNull,
                                    SqlString>;
  variant_type data;

public:
  SqlVariant(const SqlVariant& other)
    : data(other.data) {
  }

  SqlVariant& operator=(const SqlVariant& other) {
    if (this != &other) {
      data = other.data;
    }
    return *this;
  }

  explicit SqlVariant(const SqlDecimal& v)
    : data(v) {
  }

  explicit SqlVariant(variant_type v)
    : data(std::move(v)) {
  }

  explicit SqlVariant(const size_t v)
    : data(SqlBigInt(static_cast<int64_t>(v))) {
  }

  explicit SqlVariant(const int64_t v)
    : data(SqlBigInt(v)) {
  }

  explicit SqlVariant(const int32_t v)
    : data(SqlInt(v)) {
  }

  explicit SqlVariant(const int16_t v)
    : data(SqlSmallInt(v)) {
  }

  explicit SqlVariant(const int8_t v)
    : data(SqlTinyInt(v)) {
  }


  explicit SqlVariant(const double v)
    : data(SqlDouble(v)) {
  }

  explicit SqlVariant(const std::string_view s)
    : data(SqlString(std::string(s))) {
  }

  explicit SqlVariant(const char* s)
    : data(SqlString(std::string(s))) {
  }

  explicit SqlVariant(std::string s)
    : data(SqlString(s)) {
  }

  SqlVariant()
    : data(SqlNull()) {
  }

  [[nodiscard]] std::string asString() const {
    if (is<SqlString>()) {
      return get<SqlString>().get();
    }
    throw std::runtime_error("Value is not a string");
  }

  [[nodiscard]] double asDouble() const {
    if (is<SqlDouble>()) {
      return get<SqlDouble>().get();
    }
    if (is<SqlFloat>()) {
      return static_cast<double>(get<SqlFloat>().get());
    }
    throw std::runtime_error("Value is not a double or float");
  }


  [[nodiscard]] int64_t asInt8() const {
    if (is<SqlBigInt>()) {
      return get<SqlBigInt>().get();
    }
    if (is<SqlInt>()) {
      return static_cast<int64_t>(get<SqlInt>().get());
    }
    if (is<SqlSmallInt>()) {
      return static_cast<int64_t>(get<SqlSmallInt>().get());
    }
    throw std::runtime_error("Value is not an integer type");
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

  [[nodiscard]] SqlTypeKind kind() const {
    if (std::holds_alternative<SqlInt>(data)) {
      return SqlTypeKind::INT;
    }
  }
};
}