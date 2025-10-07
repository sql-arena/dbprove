#include "include/dbprove/sql/sql_type.h"
#include "dbprove/common/string.h"
#include <map>


namespace sql {
void checkTableName(const std::string_view table) {
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

SqlTypeKind to_sql_type_kind(const std::string_view type_name) {
  static const std::map<std::string_view, SqlTypeKind> m = {{"VARCHAR", SqlTypeKind::STRING},
                                                            {"CHAR", SqlTypeKind::STRING},
                                                            {"NVARCHAR", SqlTypeKind::STRING},
                                                            {"NCHAR", SqlTypeKind::STRING},
                                                            {"TEXT", SqlTypeKind::STRING},
                                                            {"NTEXT", SqlTypeKind::STRING},
                                                            {"REAL", SqlTypeKind::REAL},
                                                            {"FLOAT4", SqlTypeKind::REAL},
                                                            {"DOUBLE", SqlTypeKind::DOUBLE},
                                                            {"FLOAT8", SqlTypeKind::DOUBLE},
                                                            {"DOUBLE PRECISION", SqlTypeKind::REAL},
                                                            {"INT2", SqlTypeKind::SMALLINT},
                                                            {"SMALLINT", SqlTypeKind::SMALLINT},
                                                            {"INT", SqlTypeKind::INT},
                                                            {"INT4", SqlTypeKind::INT},
                                                            {"INTEGER", SqlTypeKind::INT},
                                                            {"INT8", SqlTypeKind::BIGINT},
                                                            {"BIGINT", SqlTypeKind::BIGINT},
                                                            {"DECIMAL", SqlTypeKind::DECIMAL},
                                                            {"DEC", SqlTypeKind::DECIMAL},
                                                            {"NUMERIC", SqlTypeKind::DECIMAL},
                                                            {"DATE", SqlTypeKind::DATE},
                                                            {"TIME", SqlTypeKind::TIME},
                                                            {"DATETIME", SqlTypeKind::DATETIME},
                                                            {"TIMESTAMP", SqlTypeKind::DATETIME},
                                                            {"TIMESTAMPTZ", SqlTypeKind::DATETIME},
                                                            {"NULL", SqlTypeKind::SQL_NULL},
                                                            {"NONE", SqlTypeKind::SQL_NULL}};
  const std::string t = to_upper(std::string(type_name));
  if (!m.contains(t)) {
    throw std::invalid_argument("Cannot mape: " + std::string(type_name) + " to SqlTypeKind");
  }
  return m.at(t);
}

int64_t SqlVariant::asInt8() const {
  if (is<SqlBigInt>()) {
    const auto i8 = std::get<SqlBigInt>(data);
    return i8.get();
  }
  if (is<SqlInt>()) {
    const auto i4 = std::get<SqlInt>(data);
    return i4.get();
  }
  if (is<SqlSmallInt>()) {
    const auto i2 = std::get<SqlSmallInt>(data);
    return i2.get();
  }
  throw std::runtime_error("Value is not an integer type");
}

int32_t SqlVariant::asInt32() const {
  const auto i8 = asInt8();
  if (i8 > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error("Value is too large for an int32");
  }
  if (i8 < std::numeric_limits<int32_t>::min()) {
    throw std::runtime_error("Value is too large for an int32");
  }
  return static_cast<int32_t>(i8);
}

int16_t SqlVariant::asInt16() const {
  const auto i8 = asInt8();
  if (i8 > std::numeric_limits<int16_t>::max()) {
    throw std::runtime_error("Value is too large for an int16");
  }
  if (i8 < std::numeric_limits<int16_t>::min()) {
    throw std::runtime_error("Value is too large for an int16");
  }
  return static_cast<int32_t>(i8);
}
}