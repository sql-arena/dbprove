#include "row.h"
#include <bit>
#include <vector>
#include <cstring>
#include "sql_exceptions.h"

namespace sql::postgres {

static constexpr Oid BOOLOID = 16;
static constexpr Oid BYTEAOID = 17;
static constexpr Oid CHAROID = 18;
static constexpr Oid INT8OID = 20;
static constexpr Oid INT2OID = 21;
static constexpr Oid INT4OID = 23;
static constexpr Oid TEXTOID = 25;
static constexpr Oid FLOAT4OID = 700;
static constexpr Oid FLOAT8OID = 701;
static constexpr Oid NUMERICOID = 1700;
static constexpr Oid VARCHAROID = 1043;
static constexpr Oid JSONOID = 114;
static constexpr Oid DATEOID = 1082;
static constexpr Oid TIMEOID = 1083;
static constexpr Oid TIMESTAMPOID = 1114;


template <typename T>
T flipByteOrder(const T v)
{
  if (std::endian::native == std::endian::little) {
    return std::byteswap(v);
  }
  return v;
}

SqlVariant parseDecimal(const char* binary_value, const int value_length) {
  if (value_length < 8) {
    throw std::runtime_error("Invalid binary data length for DECIMAL decode in Postgres");
  }

  // Decode the representation
  uint16_t num_digits, weight, sign, display_scale;
  memcpy(&num_digits, binary_value, 2);
  num_digits = flipByteOrder(num_digits);
  memcpy(&weight, binary_value + 2, 2);
  weight = flipByteOrder(weight);
  memcpy(&sign, binary_value + 4, 2);
  sign = flipByteOrder(sign);
  memcpy(&display_scale, binary_value + 6, 2);
  display_scale = flipByteOrder(display_scale);

  if (sign == 0xC000) {
    // Postgres NUMERIC can be NaN (yeah, no shit)
    return SqlVariant(SqlDecimal("NaN"));
  }
  if (num_digits == 0) {
    // Yeah, that's a thing in PG
    return SqlVariant(SqlDecimal("0"));
  }

  constexpr uint64_t base = 10000;
  bool is_negative = (sign == 0x4000);
  std::string result;
  if (is_negative) {
    result += "-";
  }

  std::vector<uint16_t> digits(num_digits);
  for (int i = 0; i < num_digits; i++) {
    memcpy(&digits[i], binary_value + (i * 2), 2);
    digits[i] = flipByteOrder(digits[i]);
  }

  std::string whole_part;
  int whole_digits = std::min(weight + 1, static_cast<int>(num_digits));

  // First, the whole part
  if (whole_digits <= 0) {
    result += "0"; // No digits to the left of decimal
  } else {
    // First digit doesn't need padding
    result += std::to_string(digits[0]);
    for (int i = 1; i < whole_digits; i++) {
      // Pad if needed
      if (digits[i] < 10) {
        result += "000";
      } else if (digits[i] < 100) {
        result += "00";
      } else if (digits[i] < 1000) {
        result += "0";
      }
      result += std::to_string(digits[i]);
    }
  }

  if (display_scale > 0) {
    result += ".";
    size_t fractional_length = result.length();
    // If weight < -1, we need leading zeros in the fractional part
    // e.g., weight = -3 means 0.00XX where XX are actual digits
    if (weight < -1) {
      // Add necessary leading zeros
      result.append((-weight - 1) * 4, '0');
    }
    for (int i = std::max(0, whole_digits); i < num_digits; i++) {
      if (digits[i] < 10) {
        result += "000";
      } else if (digits[i] < 100) {
        result += "00";
      } else if (digits[i] < 1000) {
        result += "0";
      }
      result += std::to_string(digits[i]);
    }
    fractional_length = result.length() - fractional_length;

    // Truncate or pad the fractional part based on display_scale
    if (display_scale > 0) {
      if (fractional_length > display_scale) {
        result = result.substr(0, display_scale);
      } else if (fractional_length < display_scale) {
        result.append(display_scale - fractional_length, '0');
      }
    }
  }

  return SqlVariant(SqlDecimal(result));
}

template <typename T>
SqlVariant parseInt(const char* binary_value)
{
  T int_value;
  std::memcpy(&int_value, binary_value, sizeof(T));
  return SqlVariant(flipByteOrder(int_value));
}

template <typename T>
SqlVariant parseFloat(const char* binary_value)
{
  using UIntType = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;
  UIntType raw_value;
  std::memcpy(&raw_value, binary_value, sizeof(T));
  raw_value = flipByteOrder(raw_value);
  T result;
  std::memcpy(&result, &raw_value, sizeof(T));
  return SqlVariant(result);
}


Row::~Row() {
  if (!contained_) {
    PQclear(result_);
  }
}

SqlVariant Row::get(const size_t index) const {
  const int pg_field_num = static_cast<int>(index); // To match libpq internal representation
  const Oid pg_type = PQftype(result_, pg_field_num);
  const char* binary_value = PQgetvalue(result_, row_number_, pg_field_num);
  const int value_length = PQgetlength(result_, row_number_, pg_field_num);

  switch (pg_type) {
    case INT2OID:
      return parseInt<int16_t>(binary_value);
    case INT4OID:
      return parseInt<int32_t>(binary_value);
    case INT8OID:
      return parseInt<int64_t>(binary_value);
    case FLOAT4OID:
      return parseFloat<float>(binary_value);
    case FLOAT8OID:
      return parseFloat<double>(binary_value);
    case VARCHAROID:
    case TEXTOID:
    case JSONOID:
      return SqlVariant(std::string(PQgetvalue(result_, row_number_, pg_field_num)));
    case NUMERICOID:
      return parseDecimal(binary_value, value_length);
  }
  throw InvalidTypeException("OID(" + std::to_string(pg_type) + ")");
}
}