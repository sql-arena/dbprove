#include "include/dbprove/common/json_utility.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

namespace dbprove::common {
namespace {
std::vector<uint8_t> decode_base64(std::string_view input) {
  auto decode_char = [](const char c) -> int {
    if (c >= 'A' && c <= 'Z') {
      return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
      return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
      return c - '0' + 52;
    }
    if (c == '+') {
      return 62;
    }
    if (c == '/') {
      return 63;
    }
    return -1;
  };

  std::vector<uint8_t> output;
  int buffer = 0;
  int bits = 0;
  for (const char c : input) {
    if (c == '=') {
      break;
    }
    const auto decoded = decode_char(c);
    if (decoded < 0) {
      continue;
    }
    buffer = (buffer << 6) | decoded;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      output.push_back(static_cast<uint8_t>((buffer >> bits) & 0xFF));
    }
  }
  return output;
}

std::string unsigned_int128_to_string(unsigned __int128 value) {
  if (value == 0) {
    return "0";
  }

  std::string out;
  while (value > 0) {
    const auto digit = static_cast<unsigned>(value % 10);
    out.push_back(static_cast<char>('0' + digit));
    value /= 10;
  }
  std::reverse(out.begin(), out.end());
  return out;
}
} // namespace

std::optional<int64_t> json_as_int64(const nlohmann::json& value) {
  if (value.is_number_integer()) {
    return value.get<int64_t>();
  }
  if (value.is_number_unsigned()) {
    const auto numeric = value.get<uint64_t>();
    if (numeric <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return static_cast<int64_t>(numeric);
    }
    return std::nullopt;
  }
  if (value.is_string()) {
    try {
      return std::stoll(value.get<std::string>());
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

std::optional<double> json_as_double(const nlohmann::json& value) {
  if (value.is_number()) {
    return value.get<double>();
  }
  if (value.is_string()) {
    try {
      return std::stod(value.get<std::string>());
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

std::string json_decimal128_to_string(const nlohmann::json& decimal) {
  if (!decimal.is_object() || !decimal.contains("value")) {
    return "0";
  }

  const auto bytes = decode_base64(decimal["value"].get<std::string>());
  if (bytes.empty()) {
    return "0";
  }

  unsigned __int128 raw = 0;
  for (const auto byte : bytes) {
    raw = (raw << 8U) | static_cast<unsigned __int128>(byte);
  }

  const bool negative = (raw >> 127U) != 0;
  const auto magnitude = negative ? (~raw + 1U) : raw;
  auto digits = unsigned_int128_to_string(magnitude);
  const auto scale = decimal.contains("s") ? std::stoi(decimal["s"].get<std::string>()) : 0;

  if (scale > 0) {
    if (digits.size() <= static_cast<size_t>(scale)) {
      digits.insert(0, static_cast<size_t>(scale) - digits.size() + 1, '0');
    }
    digits.insert(digits.size() - static_cast<size_t>(scale), 1, '.');
  }

  if (negative && digits != "0") {
    digits.insert(digits.begin(), '-');
  }
  return digits;
}

std::string json_date32_to_string(const int64_t days_since_epoch) {
  using namespace std::chrono;
  const sys_days day = sys_days{year{1970} / 1 / 1} + days{days_since_epoch};
  const year_month_day ymd{day};
  std::ostringstream out;
  out << "DATE '";
  out << static_cast<int>(ymd.year()) << "-";
  out << std::setw(2) << std::setfill('0') << static_cast<unsigned>(ymd.month()) << "-";
  out << std::setw(2) << std::setfill('0') << static_cast<unsigned>(ymd.day()) << "'";
  return out.str();
}

std::string json_arrow_type_to_string(const nlohmann::json& arrow_type) {
  if (!arrow_type.is_object() || arrow_type.empty()) {
    return "UNKNOWN";
  }

  const auto it = arrow_type.begin();
  const auto& name = it.key();
  if (name == "DECIMAL128") {
    const auto precision = it.value().value("precision", 0);
    const auto scale = it.value().value("scale", 0);
    return "DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
  }
  if (name == "UTF8VIEW") {
    return "STRING";
  }
  return name;
}
} // namespace dbprove::common
